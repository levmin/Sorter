/*
   Multithreaded multi-pass file sorter.
   The program generates a file comprised of a certain number of records and then sorts these records in alpha-numerical order.
*/

#include <algorithm>
#include <vector>
#include <fstream>
#include <iostream>
#include <limits>
#include <iomanip>
#include <string>
#include <memory>
#include <chrono>
#include <cstring>
#include <atomic>
#include <thread>
#include <mutex>
#include <assert.h>
#include <string>
#include <experimental/filesystem>

constexpr size_t RECORD_LENGTH = 4096;
constexpr size_t NUM_RECORDS = 200000;
constexpr size_t FILE_LENGTH = RECORD_LENGTH*NUM_RECORDS;
constexpr size_t TARGET_NUM_RECORDS_IN_BUFFER = 20000;

const size_t numCores = std::thread::hardware_concurrency();
const size_t NUM_RECORDS_IN_BUFFER = TARGET_NUM_RECORDS_IN_BUFFER / numCores * numCores;
const size_t BUFFER_SIZE = NUM_RECORDS_IN_BUFFER * RECORD_LENGTH;
const size_t core_buffer_size = NUM_RECORDS_IN_BUFFER / numCores; //core assigned buffer size in records
const size_t core_buffer_size_bytes = core_buffer_size * RECORD_LENGTH;

struct record_type
{
   char m_record[RECORD_LENGTH];

   record_type()    {
      std::memset(m_record, 0, RECORD_LENGTH);
   }
   
   const record_type& operator=(const record_type& val)   {
      std::memcpy(m_record, val.m_record, RECORD_LENGTH);
      return *this;
   }
   
   bool operator< (const record_type& val) const   {
      return std::memcmp((const char *)m_record, (const char *)val.m_record, RECORD_LENGTH) < 0;
   }
   
   bool operator<= (const record_type& val) const   {
      return std::memcmp((const char *)m_record, (const char *)val.m_record, RECORD_LENGTH) <= 0;
   }
};

using CRecordArray = std::vector<record_type>;

class CFile;
using CFilePtr = std::unique_ptr<CFile>;
class CFile :public std::fstream
{
protected:
   std::string m_name;
public:

   const char * getName() const   {
      return m_name.c_str();
   }

   CFile(const std::string& filename, ios_base::openmode mode) : std::fstream(filename, mode)   {
      m_name = filename;
   }

   virtual ~CFile()   {
      if (is_open())
         close();
   }

   void reopen()   {
      if (is_open())
         close();
      std::fstream::open(m_name, std::ios_base::in | std::ios_base::binary);
   }
};

class CTempFile : public CFile
{
   static int m_nCounter;
public:

   CTempFile(const std::string& filename, ios_base::openmode mode) : CFile(filename, mode)   {
   }
   
   static CFilePtr createTempFile()   {
      return std::make_unique<CTempFile>("_tempFile" + std::to_string(m_nCounter++) + ".tmp", std::ios_base::out | std::ios_base::binary);
   }
   
   virtual ~CTempFile()   {
      if (is_open())
         close();
      std::remove(m_name.c_str());
   }
};

int CTempFile::m_nCounter = 0;

using CFilePtrArray = std::vector<CFilePtr>;
CFilePtrArray sorted_files;

int main()
{
   static_assert(RECORD_LENGTH % sizeof(size_t) == 0, "Inappropriate RECORD_LENGTH");
   std::ifstream f_sort_input{ "sorter.tmp",  std::ios_base::in | std::ios_base::out | std::ios_base::binary };

   if (!f_sort_input.is_open())   {
   //create a sample input 
      std::ofstream f_to_be_sorted{ "sorter.tmp", std::ios_base::out | std::ios_base::binary };

      char record[RECORD_LENGTH] = { 0 };
      memset(record,  ' ', RECORD_LENGTH);
      
      const char scrambler[10] = {
         '2','6','1','9','8','0','5','3','7','4',
      };
      //the records will be filled with blanks except for a few last bytes that 
      //will have scrambled record numbers
      for (size_t i = 0; i < NUM_RECORDS; i++)      {
         std::string str = std::to_string(i);
         memcpy(record + RECORD_LENGTH - str.length(), str.c_str(), str.length());
         record[RECORD_LENGTH - 1] = scrambler[record[RECORD_LENGTH - 1] - '0'];
         f_to_be_sorted.write(record,RECORD_LENGTH);    
         memset(record + RECORD_LENGTH - str.length(), ' ', str.length());
      }
      
      f_to_be_sorted.close();
      f_sort_input.open("sorter.tmp", std::ios_base::in | std::ios_base::binary);
   }

   //start the timer to measure how long will it take to sort the sorter.tmp
   auto begin = std::chrono::steady_clock::now();

   //split the file into chunks and sort them
   CRecordArray buf(NUM_RECORDS_IN_BUFFER);

   struct CRecordPtrBase
   {
      CRecordArray::const_iterator m_ptr;

      bool operator< (const CRecordPtrBase& val) const       {
         return (*m_ptr) < (*val.m_ptr);
      }
      
      bool operator<= (const CRecordPtrBase& val) const      {
         return (*m_ptr) <= (*val.m_ptr);
      }
      
      CRecordPtrBase(CRecordArray::const_iterator ptr) : 
         m_ptr(ptr)   {
      }
   };
   
   struct CRecordPtr : public CRecordPtrBase
   {
      size_t m_coreNum;
      size_t m_numCoreRecords;
      
      CRecordPtr(CRecordArray::const_iterator ptr, const size_t coreNum, const size_t coreRecords)
         : CRecordPtrBase(ptr), m_coreNum(coreNum), m_numCoreRecords(coreRecords)   {
      }
      
      bool operator< (const CRecordPtrBase& val) const   {
         return (*m_ptr) < (*val.m_ptr);
      }
      
      bool operator<= (const CRecordPtrBase& val) const   {
         return (*m_ptr) <= (*val.m_ptr);
      }
   };

   for (size_t file_offset = 0; file_offset < FILE_LENGTH; file_offset += BUFFER_SIZE)
   {
      std::vector<std::thread> threads;

      size_t bytes_to_read = std::min(BUFFER_SIZE, FILE_LENGTH - file_offset);
      std::vector<CRecordPtr> curMergeRecords;
      static std::mutex f_sort_input_guard;

      for (size_t coreIndex = 0; coreIndex < numCores && bytes_to_read > 0; coreIndex++)      {
         const size_t core_bytes_to_read = std::min(bytes_to_read, core_buffer_size_bytes);
         const size_t core_offset = coreIndex*core_buffer_size;
         auto   core_buf_begin = buf.begin() + core_offset;
         threads.emplace_back      (
            std::thread         {
               [=,&f_sort_input,&curMergeRecords]()         {
               {//read data into a core buffer
                  std::lock_guard<std::mutex> guard(f_sort_input_guard);
                  f_sort_input.seekg(file_offset + core_offset * RECORD_LENGTH, std::ios_base::beg);
                  f_sort_input.read(&(core_buf_begin->m_record[0]), core_bytes_to_read);
                  }
               //sort them
               std::sort(core_buf_begin, core_buf_begin + core_bytes_to_read / RECORD_LENGTH);
               static std::mutex mutex;
               std::lock_guard<std::mutex> guard(mutex);
               curMergeRecords.push_back(CRecordPtr{ core_buf_begin ,coreIndex ,core_bytes_to_read / RECORD_LENGTH });
               }
            }
         );
         if (bytes_to_read >= core_bytes_to_read)
            bytes_to_read -= core_bytes_to_read;
         else
            bytes_to_read = 0;
      };
      //wait for the threads to finish
      for (std::thread& th : threads)
         th.join();
      threads.clear();

      //sort curMergeRecords
      std::sort(curMergeRecords.begin(), curMergeRecords.end());

      CFilePtr tempFile = CTempFile::createTempFile();

      //create 2 theads, producer and consumer
      //first will do the very merge, the second will write it into tempFile
      const char * writeRecordPtr = NULL;
      static std::mutex wrp_mutex;
      static bool bFinish = false;
      bFinish = false;
      threads.emplace_back      (
         std::thread      { //consumer thread
         [&writeRecordPtr,&tempFile]()       {
            while (true)             {
               while (!wrp_mutex.try_lock());
               if (writeRecordPtr)               {
                  const char * temp = writeRecordPtr;
                  writeRecordPtr = NULL;
                  wrp_mutex.unlock();

                  tempFile->write(temp, RECORD_LENGTH);
               }
               else               {
                  wrp_mutex.unlock();
                  if (bFinish)
                     return;
               }
            }
         }
      }
      );

      threads.emplace_back      (
         std::thread      {//producer thread
         [&]()         {
            while (true)         {
               if (curMergeRecords.size() == 0)      {
                  bFinish = true;
                  return;
               }
               while (!wrp_mutex.try_lock());
               if (writeRecordPtr)            {
                  wrp_mutex.unlock();
                  std::this_thread::yield();
                  continue;
               }
               auto & firstMR = curMergeRecords[0];
               writeRecordPtr = firstMR.m_ptr->m_record;
               wrp_mutex.unlock();
               firstMR.m_ptr++;
               if (firstMR.m_ptr == buf.begin() + firstMR.m_coreNum *core_buffer_size + firstMR.m_numCoreRecords)
               {//we are done with this core's results
                  curMergeRecords.erase(curMergeRecords.begin());
                  continue;
               }
               if (curMergeRecords.size() > 1)      
               {//do the merge
                  if (!(firstMR <= curMergeRecords[1]))                   {
                     if (*(curMergeRecords.end() - 1) <= firstMR)         {
                        curMergeRecords.push_back(firstMR);
                     }
                     else         {
                        //find its proper place
                        size_t lower = 1;
                        size_t upper = curMergeRecords.size() - 1;
                        while (upper - lower > 1)            {
                           size_t middle = (upper + lower) / 2;
                           if (firstMR <= curMergeRecords[middle])
                              upper = middle;
                           else
                              lower = middle;
                        }
                        curMergeRecords.insert(curMergeRecords.begin() + upper, firstMR);
                     }
                     curMergeRecords.erase(curMergeRecords.begin());
                  }
               }
               }
            }
         }
         );
         //wait for the threads to finish
         for (std::thread& th : threads)
            th.join();
         threads.clear();
         tempFile->reopen();
         sorted_files.emplace_back(std::move(tempFile));
      }
   
      //do a 2 pass merge of sorted temp files
      auto mergeFiles = [&buf](size_t filesToMerge, const char * fileName = NULL) -> CFilePtr
      {
         assert((filesToMerge > 0));
         assert(filesToMerge <= sorted_files.size());
         if (filesToMerge == 1)          {
            if (fileName)             {
               sorted_files[0]->close();
               std::experimental::filesystem::copy_file(sorted_files[0]->getName(), fileName);
               return std::make_unique<CFile>(fileName, std::ios_base::out | std::ios_base::binary);
            }
            else {
               return std::move(sorted_files[0]);
            }
         }
         else    {// do the real merge
            CFilePtr f_out = (fileName
               ? std::make_unique<CFile>(fileName, std::ios_base::out | std::ios_base::binary)
               : CTempFile::createTempFile());
            const auto fileBufSize = NUM_RECORDS_IN_BUFFER / filesToMerge;

            //create a vector of file chunk pointers
            struct CChunk : public CRecordPtrBase
            {
               CRecordArray::const_iterator m_end;
               size_t m_sortedFileIndex;
               CChunk(CRecordArray::const_iterator ptr, CRecordArray::const_iterator end, size_t index)
                  :CRecordPtrBase(ptr), m_end(end), m_sortedFileIndex(index)      {}
            };
            std::vector<CChunk> chunks;
            //initialize and sort it
            for (size_t index = 0; index < filesToMerge; index++)      {
               sorted_files[index]->read(
                  buf.begin()->m_record + fileBufSize*RECORD_LENGTH*index, fileBufSize*RECORD_LENGTH);
               if (sorted_files[index]->gcount() >= std::streamsize(RECORD_LENGTH))
                  chunks.push_back(CChunk{ buf.begin() + fileBufSize*index, buf.begin() + fileBufSize*index + 
                     static_cast<size_t>(sorted_files[index]->gcount()) / RECORD_LENGTH, index });
            }
            std::sort(chunks.begin(),chunks.end());

            //create 2 theads, producer and consumer
            //the first will do the very merge, the second will write the result into f_out
            std::vector<std::thread> threads;
            
            static const char * writeRecordPtr = NULL;
            writeRecordPtr = NULL;
            static std::mutex wrp_mutex;
            static bool bFinish = false;
            bFinish = false;
            threads.emplace_back          (
               std::thread         { //consumer thread
               [&]()            {
               while (true)      {
                  while (!wrp_mutex.try_lock());
                  if (writeRecordPtr)               {
                     const char * temp = writeRecordPtr;
                     writeRecordPtr = NULL;
                     wrp_mutex.unlock();
                     f_out->write(temp, RECORD_LENGTH);
                  }
                  else                  {
                     wrp_mutex.unlock();
                     if (bFinish)
                        return;
                  }
               }
            }
            }
            );

            threads.emplace_back      (
               std::thread         { //producer thread
               [&]()         {
               while (true)         {
                  if (chunks.size() == 0)         {
                     bFinish = true;
                     return;
                  }
                  while (!wrp_mutex.try_lock());
                  if (writeRecordPtr)      {
                     wrp_mutex.unlock();
                     std::this_thread::yield();
                     continue;
                  }
                  writeRecordPtr = chunks[0].m_ptr->m_record;
                  wrp_mutex.unlock();

                  bool bFileEnded = false;
                  if (++(chunks[0].m_ptr) == chunks[0].m_end)
                  {//try to get more data
                     size_t index = chunks[0].m_sortedFileIndex;
                     sorted_files[index]->read(buf.begin()->m_record + fileBufSize*index*RECORD_LENGTH, fileBufSize*RECORD_LENGTH);
                     size_t bytesRead = static_cast<size_t>(sorted_files[index]->gcount());
                     if (bytesRead >= RECORD_LENGTH)   {
                        chunks[0].m_ptr = buf.begin() + index*fileBufSize;
                        chunks[0].m_end = buf.begin() + index*fileBufSize + bytesRead / RECORD_LENGTH;
                     }
                     else    {//the sorted_files[index] has ended
                        bFileEnded = true;
                        chunks.erase(chunks.begin());
                     }
                  }

                  if (!bFileEnded && chunks.size() > 1)
                  {//do the merge
                     if (!(chunks[0] <= chunks[1]))                {
                        if (*(chunks.end() - 1) <= chunks[0])         {
                           chunks.push_back(chunks[0]);
                        }
                        else {//find its proper place
                           size_t lower = 1;
                           size_t upper = chunks.size() - 1;
                           while (upper - lower > 1)   {
                              size_t middle = (upper +lower) / 2;
                              if (chunks[0] <= chunks[middle])
                                 upper = middle;
                              else
                                 lower = middle;
                           }
                           chunks.insert(chunks.begin() + upper, chunks[0]);
                        }
                        chunks.erase(chunks.begin());
                     }
                  }
               }
            }
            }
            );
            //wait for the threads to finish
            for (std::thread& th : threads)
               th.join();
            threads.clear();
            if (fileName)
               f_out->close();
            else
               f_out->reopen();
            return f_out;
         }
      };

      //do a 2 pass file merge
      if (sorted_files.size()>1)   {//first pass
         std::vector<CFilePtr> sorted_files2;
         auto firstPassConsolidationFactor = std::max(static_cast<size_t>(floor(sqrt(sorted_files.size()))), static_cast<size_t>(2U));
         while (sorted_files.size())          {
            auto filesToMerge = std::min(firstPassConsolidationFactor, sorted_files.size());
            sorted_files2.emplace_back(mergeFiles(filesToMerge));
            sorted_files.erase(sorted_files.begin(), sorted_files.begin() + filesToMerge);
         }
         sorted_files = std::move(sorted_files2);
      }
      //second pass
      mergeFiles(sorted_files.size(), "output.tmp");
         
      auto end = std::chrono::steady_clock::now();
      std::cout << "The sorting took " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << " milliseconds\n";

      //validate that sorter.tmp and output.tmp have the same length

      if (std::experimental::filesystem::file_size("sorter.tmp") != std::experimental::filesystem::file_size("output.tmp"))
      {
         std::cout << "Output has different length than input!\n";
         return -1;
      }

      //validate that the output is indeed sorted

      record_type r1, r2;
      std::fstream fin("output.tmp", std::ios_base::binary | std::ios_base::in);
      fin.read(&r1.m_record[0], RECORD_LENGTH);
      while (!fin.eof())      {
         fin.read(&r2.m_record[0], RECORD_LENGTH);
         if (fin.gcount() == std::streamsize(RECORD_LENGTH))      {
            if (!(r1 <= r2))         {
               std::cout << "Violation!\n";
               return -1;
            }
            r1 = r2;
         }
      }

      std::cout << "Validation succeeded!\n";

      return 0;
}


