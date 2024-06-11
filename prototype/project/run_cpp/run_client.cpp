#include "client.h"
#include "lrc.h"
#include "toolbox.h"
#include <fstream>
#include <regex>
#include <sys/time.h>
#include <unistd.h>
#include <sys/stat.h>

inline int rand_num(int range)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis(0, range - 1);
    int num = dis(gen);
    return num;
};

inline int rand_num_in_range(int min_v, int max_v)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis(min_v, max_v);
    int num = dis(gen);
    return num;
};

template <typename T>
void getArrayStats(T arr[], int len, T& avg, T& max, T& min, T& tot) {
    if (len == 0) {
        return;
    }
    max = -1;
    min = T(INT_MAX);
    T sum = 0;
    for (int i = 0; i < len; i++) {
        sum += arr[i];
        if (arr[i] > max) {
            max = arr[i];
        }
        if (arr[i] < min) {
            min = arr[i];
        }
    }
    avg = sum / len;
    tot = sum;
}

void print_runtime_result(double cost[], int cross_cluster[], int r, int avg_num)
{
  std::cout << "   <Simulation, Runtime>  Cross Cluster: " << cross_cluster[r] << " blocks." << std::endl;
  std::cout << "   <Simulation, Block Average> Cross Cluster: " << cross_cluster[r] / avg_num << " blocks." << std::endl;
  std::cout << "   Runtime Cost: " << cost[r]  << "s." << std::endl;
  std::cout << "   Block Average Cost: " << cost[r] / avg_num << "s." << std::endl;
}

void print_average_result(double cost[], int cross_cluster[], int repaired_block[], int len, int block_size, bool flag)
{
  double max_cost = 0.0, min_cost = 0.0, avg_cost = 0.0, tot_cost = 0.0;
  int max_repaired = 0, min_repaired = 0, avg_repaired = 0, tot_repaired = 0;
  int max_cross_cluster = 0, min_cross_cluster = 0, avg_cross_cluster = 0, tot_cross_cluster = 0;
  getArrayStats(cost, len, avg_cost, max_cost, min_cost, tot_cost);
  getArrayStats(cross_cluster, len, avg_cross_cluster, max_cross_cluster, min_cross_cluster, tot_cross_cluster);
  getArrayStats(repaired_block, len, avg_repaired, max_repaired, min_repaired, tot_repaired);
  std::cout << "Average Result:" << std::endl;
  std::cout << "   <Simulation, " << len << "-runtimes Total> Cross Cluster: " << tot_cross_cluster << std::endl;
  std::cout << "   <Simulation, Runtime Average> Cross Cluster: (" << avg_cross_cluster << ", " << max_cross_cluster << ", " << min_cross_cluster << ")blocks." << std::endl;
  std::cout << "   <Simulation, Block Average> Cross Cluster: (" << tot_cross_cluster / tot_repaired << ", " 
            << max_cross_cluster / avg_repaired << ", " << min_cross_cluster / avg_repaired << ")blocks." << std::endl;
  std::cout << "   Average Repaired Block Num: Average: " << avg_repaired << ", Max: " << max_repaired << ", Min: " << min_repaired << "." << std::endl;
  std::cout << "   Total " << len << "-runtimes Cost: " << tot_cost << "s.\n";
  std::cout << "   Average Runtime Cost: Average: " << avg_cost << "s, Max: " << max_cost << "s, Min: " << min_cost << "s." << std::endl;
  std::cout << "   Average Block Cost: Average: " << tot_cost / tot_repaired << "s, Max: " << max_cost / avg_repaired << "s, Min: " << min_cost / avg_repaired << "s." << std::endl;
  if(flag)
  {
    double avg_repair_rates = avg_repaired * double(block_size) / avg_cost;
    double max_repair_rates = max_repaired * double(block_size) / max_cost;
    double min_repair_rates = min_repaired * double(block_size) / min_cost;
    std::cout << "   Repair Rate: Average: " << avg_repair_rates << "s, Max: " << max_repair_rates << "s, Min: " << min_repair_rates << "." << std::endl;
  }
}

void parse_tracefile(std::string tracefile_path, int block_size, std::vector<float> &ms_x, std::vector<int> &ms_g, std::vector<std::vector<int>> &ms_object_sizes, std::vector<std::vector<int>> &ms_object_accessrates)
{
    std::ifstream file(tracefile_path);
    if (file.is_open()) {
        std::string line;
        while (std::getline(file, line)) {
            std::vector<int> object_sizes;
            std::vector<int> object_accessrates;
            std::regex pattern1("(\\d+\\.\\d+),(\\d+)");
            std::sregex_iterator it1(line.begin(), line.end(), pattern1);
            std::sregex_iterator end1;
            while (it1 != end1) {
                std::smatch match = *it1;
                ms_x.push_back(std::stof(match[1].str()));
                ms_g.push_back(std::stoi(match[2].str()));
                ++it1;
            }
            std::regex pattern2("\\((\\d+),(\\d+)\\)");
            std::sregex_iterator it2(line.begin(), line.end(), pattern2);
            std::sregex_iterator end2;
            while (it2 != end2) {
                std::smatch match = *it2;
                object_sizes.push_back(std::stoi(match[1].str()) * block_size);
                object_accessrates.push_back(std::stoi(match[2].str()));
                ++it2;
            }
            ms_object_sizes.push_back(object_sizes);
            ms_object_accessrates.push_back(object_accessrates);
        }
        file.close();
    } else {
        std::cerr << "Failed to open the file." << std::endl;
    }
}

int main(int argc, char **argv)
{
  if (argc != 11)
  {
    std::cout << "./run_client test_type partial_decoding encode_type singlestripe_placement_type global_parity_num storage_overhead block_size(KB) stripe_num is_range tracefilename" << std::endl;
    std::cout << "./run_client false false U_LRC Opt 3 1.3 65536 4 false tracefile.txt" << std::endl;
    exit(-1);
  }

  bool test_type, partial_decoding, is_range;
  ECProject::EncodeType encode_type;
  ECProject::PlacementType s_placement_type;
  int k, l, g_m, r, n;
  int stripe_num, block_size;
  float x;

  char buff[256];
  getcwd(buff, 256);
  std::string cwf = std::string(argv[0]);

  test_type = (std::string(argv[1]) == "true");
  partial_decoding = (std::string(argv[2]) == "true");
  if (std::string(argv[3]) == "U_LRC")
  {
    encode_type = ECProject::U_LRC;
  }
  else if(std::string(argv[3]) == "NU_LRC")
  {
    encode_type = ECProject::NU_LRC;
  }
  else if(std::string(argv[3]) == "Azu_LRC")
  {
    encode_type = ECProject::Azu_LRC;
  }
  else
  {
    std::cout << "error: unknown encode_type" << std::endl;
    exit(-1);
  }
  if (std::string(argv[4]) == "Opt")
  {
    s_placement_type = ECProject::Opt;
  }
  else if (std::string(argv[4]) == "Flat")
  {
    s_placement_type = ECProject::Flat;
  }
  else if (std::string(argv[4]) == "Ran")
  {
    s_placement_type = ECProject::Ran;
  }
  else
  {
    std::cout << "error: unknown singlestripe_placement_type" << std::endl;
    exit(-1);
  }
  g_m = std::stoi(std::string(argv[5]));
  x = std::stof(std::string(argv[6]));
  block_size = std::stoi(std::string(argv[7])) * 1024;
  stripe_num = std::stoi(std::string(argv[8]));
  is_range = (std::string(argv[9]) == "true");
  std::string tracefile_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../../tracefile/" + std::string(argv[10]);

  std::vector<ECProject::fp> cp;
  std::vector<std::vector<int>> ms_object_sizes;
  std::vector<std::vector<int>> ms_object_accessrates;
  std::vector<float> ms_x;
  std::vector<int> ms_g;
  parse_tracefile(tracefile_path, block_size, ms_x, ms_g, ms_object_sizes, ms_object_accessrates);
  
  n = int(ms_object_sizes[0].size());
  if(!test_type)
  {
    if(encode_type == ECProject::U_LRC)
    {
      ECProject::generate_coding_parameters_for_stripe_U_LRC(k, r, l, g_m, x, block_size, ms_object_sizes[0]);
    }
    else if(encode_type == ECProject::NU_LRC)
    {
      ECProject::generate_coding_parameters_for_stripe_NU_LRC(k, r, l, g_m, x, block_size, cp, ms_object_sizes[0], ms_object_accessrates[0]);
    }
    else
    {
      ECProject::generate_coding_parameters_for_stripe_Azu_LRC(k, r, l, g_m, x, block_size, ms_object_sizes[0]);
    }
  }
  else
  {
    if(encode_type == ECProject::Azu_LRC)
    {
      ECProject::generate_coding_parameters_for_stripe_Azu_LRC(k, r, l, g_m, x, block_size, ms_object_sizes[0]);
    }
    else
    {
      ECProject::generate_coding_parameters_for_stripe_U_LRC(k, r, l, g_m, x, block_size, ms_object_sizes[0]);
    }
  }


  std::string client_ip = "0.0.0.0", coordinator_ip = "0.0.0.0";

  ECProject::Client client(client_ip, 44444, coordinator_ip + std::string(":55555"));
  std::cout << client.sayHelloToCoordinatorByGrpc("Client") << std::endl;


  if (client.SetParameterByGrpc({test_type, partial_decoding, encode_type, s_placement_type, n, k, r, g_m, x, block_size, cp}))
  {
    std::cout << "set parameter successfully!" << std::endl;
  }
  else
  {
    std::cout << "Failed to set parameter!" << std::endl;
  }

  struct timeval s_start_time, s_end_time;
  struct timeval g_start_time, g_end_time;
  double s_time = 0.0, g_time = 0.0;
  // set
  std::cout << "[SET BEGIN]" << std::endl;
  gettimeofday(&s_start_time, NULL);
  int obj_idx = 0;
  int tot_val_len = 0;
  for (int i = 0; i < stripe_num; i++)
  {
    std::string filename = "Object";
    std::vector<std::string> object_keys;
    std::vector<int> object_sizes = ms_object_sizes[0];
    std::vector<int> object_accessrates = ms_object_accessrates[0];
    int tmp_n = n;
    int val_len = 0;
    if(test_type)
    {
      tmp_n = int(ms_object_sizes[i].size());
      object_sizes = ms_object_sizes[i];
      object_accessrates = ms_object_accessrates[i];
    }
    for(int j = 0; j < tmp_n; j++)
    {
      std::string object_key = "obj_" + std::to_string(obj_idx);
      object_keys.push_back(object_key);
      val_len += object_sizes[j];
      obj_idx++;
    }
    float xx = x;
    int gg = g_m;
    if(is_range)
    {
      xx = ms_x[i];
      gg = ms_g[i];
    }
    std::string readpath = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../../data/" + filename;
    if (access(readpath.c_str(), 0) == -1)
    {
      std::cout << "[Client] file does not exist!" << std::endl;
      exit(-1);
    }
    else
    {
      std::string key = "stripe_" + std::to_string(i);
      char *buf = new char[val_len];
      buf[val_len] = '\0';
      std::ifstream ifs(readpath);
      ifs.read(buf, val_len);
      client.set(key, std::string(buf), object_keys, object_sizes, object_accessrates, xx, gg);
      ifs.close();
      delete buf;
    }
    tot_val_len += val_len;
  }
  gettimeofday(&s_end_time, NULL);
  std::cout << "[SET END]" << std::endl
            << std::endl;

  // get
  // std::cout << "[GET BEGIN]" << std::endl;
  // gettimeofday(&g_start_time, NULL);
  // for (int i = 0; i < obj_idx; i++)
  // {
  //   std::string object_key = "obj_" + std::to_string(i);
  //   std::string object_value;
  //   client.get(object_key, object_value);
  // }
  // gettimeofday(&g_end_time, NULL);
  // std::cout << "[GET END]" << std::endl
  //           << std::endl;

  // s_time = s_end_time.tv_sec - s_start_time.tv_sec + (s_end_time.tv_usec - s_start_time.tv_usec) * 1.0 / 1000000;
  // g_time = g_end_time.tv_sec - g_start_time.tv_sec + (g_end_time.tv_usec - g_start_time.tv_usec) * 1.0 / 1000000;
  // std::cout << "Throughput:" << std::endl;
  // std::cout << "Write: " << float(tot_val_len) / float(1024 * s_time) << "MB/s, time =" << s_time << "s." << std::endl;
  // std::cout << "Read: " << float(tot_val_len) / float(1024 * g_time) << "MB/s, time =" << g_time << "s." << std::endl;
  // std::cout << std::endl;

  if(encode_type == ECProject::U_LRC)
  {
    std::cout << "EncodeType: Uniform LRC" << std::endl;
  }
  else if(encode_type == ECProject::NU_LRC)
  {
    std::cout << "EncodeType: Non-Uniform LRC" << std::endl;
  }
  else
  {
    std::cout << "EncodeType: Azure LRC" << std::endl;
  }
  std::cout << "Placement type: " << std::string(argv[4]) << std::endl;
  std::cout << "Storage overhead: " << x << std::endl;
  std::cout << "Encode-and-Transfer: ";
  if(partial_decoding)
  {
    std::cout << "Yes" << std::endl;
  }
  else
  {
    std::cout << "No" << std::endl;
  } 
  std::cout << "Tracefile: " << std::string(argv[10]) << std::endl;
  std::cout << "Number of stripes: " << stripe_num << std::endl;
  std::cout << "Block size: " << block_size / 1024 << "KiB" << std::endl;

  double temp_cost = 0.0;
  int temp_failed = 0;
  int temp_cross_cluster = 0;
  std::vector<double> costs;
  std::vector<int> cross_clusters;
  std::vector<int> failed_blocks;
  std::vector<int> repaired_blocks;
  int run_time = 5;

  // repair

  // degraded read
  // test 1
  std::cout << "------------- For TEST 1 --------------" << std::endl;
  costs.clear();
  cross_clusters.clear();
  failed_blocks.clear();
  repaired_blocks.clear();
  for(int i = 0; i < run_time; i++)
  {
    double r_cost = 0.0;
    int r_cross_cluster = 0;
    int r_failed_block = 0;
    int r_repaired_block = 0;
    for(int j = 0; j < stripe_num; j++)
    {
      int n_files = int(ms_object_sizes[0].size());
      std::vector<int> object_sizes = ms_object_sizes[0];
      std::vector<int> object_accessrates = ms_object_accessrates[0];
      if(test_type)
      {
        object_sizes = ms_object_sizes[j];
        object_accessrates = ms_object_accessrates[j];
      }
      int lowerbound = 0, upperbound = 0;
      for(int ii = 0; ii < n_files; ii++)
      {
        int file_len = object_sizes[ii] / block_size;
        upperbound += file_len;
        for(int jj = 0; jj < object_accessrates[ii]; jj++)  // proportion to access frequency
        {
          int ran_fblock_id = rand_num(k);  // each data block has the same probability of being failed
          if(ran_fblock_id >= lowerbound && ran_fblock_id < upperbound) // a block in requested file failed
          {
            std::vector<int> failed_block_list;
            failed_block_list.push_back(ran_fblock_id);
            temp_failed = client.blocks_repair(failed_block_list, j, temp_cost, temp_cross_cluster);
            r_repaired_block += 1;
            r_cost += temp_cost;
            r_cross_cluster += temp_cross_cluster;
            r_failed_block += temp_failed;
          }
        }
        lowerbound += file_len;
      }
    }
    costs.push_back(r_cost);
    cross_clusters.push_back(r_cross_cluster);
    failed_blocks.push_back(r_failed_block);
    repaired_blocks.push_back(r_repaired_block);
    std::cout << "Runtime " << i + 1 << std::endl;
    print_runtime_result(costs.data(), cross_clusters.data(), i, r_repaired_block);
  }
  print_average_result(costs.data(), cross_clusters.data(), repaired_blocks.data(), run_time, block_size, false);

  // test 2
  std::cout << "\n------------- For TEST 2 --------------" << std::endl;
  costs.clear();
  cross_clusters.clear();
  failed_blocks.clear();
  repaired_blocks.clear();
  for(int i = 0; i < run_time; i++)
  {
    double r_cost = 0.0, l_cost = 0.0;
    int r_cross_cluster = 0;
    int r_failed_block = 0;
    int r_repaired_block = 0;
    for(int j = 0; j < stripe_num; j++)
    {
      int n_files = int(ms_object_sizes[0].size());
      std::vector<int> object_sizes = ms_object_sizes[0];
      std::vector<int> object_accessrates = ms_object_accessrates[0];
      if(test_type)
      {
        object_sizes = ms_object_sizes[j];
        object_accessrates = ms_object_accessrates[j];
      }
      int lowerbound = 0, upperbound = 0;
      for(int ii = 0; ii < n_files; ii++)
      {
        int file_len = object_sizes[ii] / block_size;
        upperbound += file_len;
        int ar = object_accessrates[ii];
        for(int kk = lowerbound; kk < upperbound; kk++)
        {
          std::vector<int> failed_block_list;
          failed_block_list.push_back(kk);
          temp_failed = client.blocks_repair(failed_block_list, j, temp_cost, temp_cross_cluster);
          r_repaired_block += 1 * ar;
          r_cost += temp_cost * ar;
          r_cross_cluster += temp_cross_cluster * ar;
          r_failed_block += temp_failed * ar;
        }
        lowerbound += file_len;
      }
      l_cost = r_cost;
    }
    costs.push_back(r_cost);
    cross_clusters.push_back(r_cross_cluster);
    failed_blocks.push_back(r_failed_block);
    repaired_blocks.push_back(r_repaired_block);
    std::cout << "Runtime " << i + 1 << std::endl;
    print_runtime_result(costs.data(), cross_clusters.data(), i, r_repaired_block);
  }
  print_average_result(costs.data(), cross_clusters.data(), repaired_blocks.data(), run_time, block_size, false);

  for(int i = 0; i < run_time; i++)
  {
    double r_cost = 0.0;
    int r_cross_cluster = 0;
    int r_failed_block = 0;
    int r_repaired_block = 0;
    for(int i = 0; i < stripe_num; i++)
    {
      int n_files = int(ms_object_sizes[0].size());
      std::vector<int> object_sizes = ms_object_sizes[0];
      std::vector<int> object_accessrates = ms_object_accessrates[0];
      if(test_type)
      {
        n_files = int(ms_object_sizes[i].size());
        object_sizes = ms_object_sizes[i];
        object_accessrates = ms_object_accessrates[i];
      }
      std::map<int, int> b2f;
      int ak = 0;
      int kk = 0;
      int b_idx = 0;
      for(int j = 0; j < n_files; j++)
      {
        int ki = object_sizes[j] / block_size;
        ak += ki * object_accessrates[j];
        kk += ki;
        for(int jj = 0; jj < ki; jj++)
        {
          b2f[b_idx] = j;
          b_idx += 1;
        }
      }
      int a_avg = ak / kk;
      std::vector<ECProject::fp> tmp_cp;
      float tmp_x = x;
      std::vector<std::vector<int>> groups;
      std::map<int, int> b2g;
      int rr = r;
      if(encode_type == ECProject::U_LRC)
      {
        ECProject::generate_coding_parameters_for_stripe_U_LRC(k, rr, l, g_m, tmp_x, block_size, object_sizes);
        ECProject::generate_stripe_information_for_U_LRC(k, rr, g_m, groups, b2g);
      }
      else if(encode_type == ECProject::NU_LRC)
      {
        ECProject::generate_coding_parameters_for_stripe_NU_LRC(k, rr, l, g_m, tmp_x, block_size, tmp_cp, object_sizes, object_accessrates);
        ECProject::generate_stripe_information_for_NU_LRC(k, g_m, groups, b2g, tmp_cp);
      }

      for (int ii = 0; ii < kk; ii++)
      {
        for(int jj = ii + 1; jj < kk; jj++)
        {
          int ran_num = rand_num(100);
          if(ran_num < 20)
          {
            std::vector<int> failed_block_list;
            r_repaired_block += 1;
            if(b2g[ii] == b2g[jj])  // one global
            {
              failed_block_list.push_back(ii);
              failed_block_list.push_back(jj);
              temp_failed = client.blocks_repair(failed_block_list, i, temp_cost, temp_cross_cluster);
              int weight = (object_accessrates[b2f[ii]] + object_accessrates[b2f[jj]]) / (2 * a_avg);
              r_cost += temp_cost * weight;
              r_cross_cluster += temp_cross_cluster * weight;
              r_failed_block += temp_failed * weight;
            }
            else  //  two local
            {
              failed_block_list.push_back(ii);
              temp_failed = client.blocks_repair(failed_block_list, i, temp_cost, temp_cross_cluster);
              // std::cout << ii << "-" << jj << " : " << temp_cost << "-";
              r_cost += temp_cost * object_accessrates[b2f[ii]]  / a_avg;
              r_cross_cluster += temp_cross_cluster * object_accessrates[b2f[ii]]  / a_avg;
              r_failed_block += temp_failed * object_accessrates[b2f[ii]]  / a_avg;

              failed_block_list.pop_back();
              failed_block_list.push_back(jj);
              temp_failed = client.blocks_repair(failed_block_list, i, temp_cost, temp_cross_cluster);
              r_cost += temp_cost * object_accessrates[b2f[jj]]  / a_avg;
              r_cross_cluster += temp_cross_cluster * object_accessrates[b2f[jj]]  / a_avg;
              r_failed_block += temp_failed * object_accessrates[b2f[jj]]  / a_avg;
            }
          }
        }
      }
    }
    costs.push_back(r_cost);
    cross_clusters.push_back(r_cross_cluster);
    failed_blocks.push_back(r_failed_block);
    repaired_blocks.push_back(r_repaired_block);
    std::cout << "Runtime " << i + 1 << std::endl;
    print_runtime_result(costs.data(), cross_clusters.data(), i, r_repaired_block);
  }
  print_average_result(costs.data(), cross_clusters.data(), repaired_blocks.data(), run_time, block_size, false);

  // node repair
  for(int i = 0; i < run_time; i++)
  {
    double r_cost = 0.0;
    int r_cross_cluster = 0;
    int r_failed_block = 0;
    int r_repaired_block = 0;
    for(int i = 0; i < stripe_num; i++)
    {
      int n_files = int(ms_object_sizes[0].size());
      std::vector<int> object_sizes = ms_object_sizes[0];
      std::vector<int> object_accessrates = ms_object_accessrates[0];
      if(test_type)
      {
        n_files = int(ms_object_sizes[i].size());
        object_sizes = ms_object_sizes[i];
        object_accessrates = ms_object_accessrates[i];
      }
      int ak = 0;
      int kk = 0;
      for(int j = 0; j < n_files; j++)
      {
        ak += (object_sizes[j] / block_size) * object_accessrates[j];
        kk += object_sizes[j] / block_size;
      }
      int a_avg = ak / kk;
      int ll = int(round(x * float(kk))) - kk - g_m;
      int rr = 0;
      if(encode_type == ECProject::Azu_LRC)
      {
        rr = (kk + ll - 1) / ll;
        ll = (kk + rr - 1) / rr;
      }
      else
      {
        rr = (kk + g_m + ll - 1) / ll;
        ll = (kk + g_m + rr - 1) / rr;
      }
      int lowerbound = 0, upperbound = 0;
      for(int ii = 0; ii < n_files; ii++)
      {
        int file_len = object_sizes[ii] / block_size;
        upperbound += file_len;
        int ar = object_accessrates[ii];
        for(int jj = lowerbound; jj < upperbound; jj++)
        {
          std::vector<int> failed_block_list;
          failed_block_list.push_back(jj);
          temp_failed = client.blocks_repair(failed_block_list, i, temp_cost, temp_cross_cluster);
          r_repaired_block += 1 * ar;
          r_cost += temp_cost * ar;
          r_cross_cluster += temp_cross_cluster * ar;
          r_failed_block += temp_failed * ar;
        }
        lowerbound += file_len;
      }
      for (int ii = kk; ii < kk + g_m + ll; ii++)
      {
        std::vector<int> failed_block_list;
        failed_block_list.push_back(ii);
        temp_failed = client.blocks_repair(failed_block_list, i, temp_cost, temp_cross_cluster);
        r_repaired_block += 1 * a_avg;
        r_cost += temp_cost * a_avg;
        r_cross_cluster += temp_cross_cluster * a_avg;
        r_failed_block += temp_failed * a_avg;
      }
    }
    costs.push_back(r_cost);
    cross_clusters.push_back(r_cross_cluster);
    failed_blocks.push_back(r_failed_block);
    repaired_blocks.push_back(r_repaired_block);
    std::cout << "Runtime " << i + 1 << std::endl;
    print_runtime_result(costs.data(), cross_clusters.data(), i, r_repaired_block);
  }
  print_average_result(costs.data(), cross_clusters.data(), repaired_blocks.data(), run_time, block_size, true);

  std::cout << "[DEL BEGIN]" << std::endl;
  client.delete_all_stripes();
  std::cout << "[DEL END]" << std::endl
            << std::endl;
  return 0;
}