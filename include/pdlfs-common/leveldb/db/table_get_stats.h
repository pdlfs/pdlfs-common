#pragma once
// Maybe this file should not be at this directory, but I
// have no idea where to put it

struct TableGetStats {
  int index_block_reads;
  int index_block_cache_hits;
  int data_block_reads;
  int data_block_cache_hits;
  TableGetStats():
      index_block_reads(0),
      index_block_cache_hits(0),
      data_block_reads(0),
      data_block_cache_hits(0) {}
};