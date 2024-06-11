#ifndef META_DEFINITION
#define META_DEFINITION
#include "devcommon.h"
namespace ECProject
{
  enum OpperateType
  {
    SET,
    GET,
    DEL,
    REPAIR
  };
  enum EncodeType
  {
    Azu_LRC,
    U_LRC,
    NU_LRC
  };
  enum PlacementType
  {
    Ran,
    Flat,
    Opt
  };
  enum FailureType
  {
    Single_Block,
    Multi_Blocks
  };

  typedef unsigned short ushort;

  typedef struct fp
  {
    ushort ki;
    ushort ri;
  } fp;

  typedef struct Block
  {
    int block_id;          // to denote the order of data blocks in a stripe
    std::string block_key; // to distinct block, globally unique
    int block_size;
    std::string map2key;
    int map2group, map2stripe, map2node, map2cluster;
    Block(int block_id, const std::string block_key, int block_size, int map2group,
          int map2stripe, int map2node, int map2cluster)
        : block_id(block_id), block_key(block_key), block_size(block_size), map2group(map2group), map2stripe(map2stripe), map2node(map2node), map2cluster(map2cluster) {}
    Block() = default;
  } Block;

  typedef struct Cluster
  {
    int cluster_id;
    std::string proxy_ip;
    int proxy_port;
    std::vector<int> nodes;
    std::vector<Block *> blocks;
    std::unordered_set<int> stripes;
    Cluster(int cluster_id, const std::string &proxy_ip, int proxy_port) : cluster_id(cluster_id), proxy_ip(proxy_ip), proxy_port(proxy_port) {}
    Cluster() = default;
  } Cluster;

  typedef struct Node
  {
    int node_id;
    std::string node_ip;
    int node_port;
    int map2cluster;
    std::unordered_map<int, int> stripes;
    Node(int node_id, const std::string node_ip, int node_port, int cluster_id) : node_id(node_id), node_ip(node_ip), node_port(node_port), map2cluster(cluster_id) {}
    Node() = default;
  } Node;

  typedef struct Stripe
  {
    int stripe_id;
    int k, r, g_m;
    std::vector<fp> cp;
    std::vector<std::string> object_keys;
    std::vector<int> object_sizes;
    std::vector<Block *> blocks;
    std::unordered_set<int> place2clusters;
  } Stripe;

  typedef struct ECSchema
  {
    ECSchema() = default;
    ECSchema(bool testtype, bool partial_decoding, EncodeType encodetype, PlacementType placementtype, int n_fileperstripe,
             int k_datablock, int r_avggroupsize, int g_m_globalparityblock, float x_storageoverhead, 
             int s_blocksizebytes, std::vector<fp> &tcp)
        : testtype(testtype), partial_decoding(partial_decoding), encodetype(encodetype), s_placementtype(placementtype),
          n_fileperstripe(n_fileperstripe), k_datablock(k_datablock), r_avggroupsize(r_avggroupsize),
          g_m_globalparityblock(g_m_globalparityblock), x_storageoverhead(x_storageoverhead), s_blocksizebytes(s_blocksizebytes)
    {
      if(encodetype == Azu_LRC)
      {
        l_localparityblock = (k_datablock + r_avggroupsize - 1) / r_avggroupsize;
      }
      else
      {
        l_localparityblock = (k_datablock + g_m_globalparityblock + r_avggroupsize - 1) / r_avggroupsize;
      }
      
      if (!testtype && encodetype == NU_LRC)
      {
        for (fp tmp : tcp)
        {
          cp.push_back(tmp);
        }
      }
    }
    bool testtype;
    bool partial_decoding;
    EncodeType encodetype;
    PlacementType s_placementtype;
    int n_fileperstripe;
    int k_datablock;
    int r_avggroupsize;
    int l_localparityblock;
    int g_m_globalparityblock;
    float x_storageoverhead;
    int s_blocksizebytes;
    std::vector<fp> cp;
  } ECSchema;
} // namespace ECProject

#endif // META_DEFINITION