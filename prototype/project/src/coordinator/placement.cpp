#include "coordinator.h"
#include "tinyxml2.h"
#include <random>
#include <unistd.h>
#include "lrc.h"
#include <sys/time.h>

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

namespace ECProject
{
    int CoordinatorImpl::generate_placement_for_LRC(int stripe_id, int block_size)
    {
        bool testtype = m_encode_parameters.testtype;
        Stripe &stripe_info = m_stripe_table[stripe_id];
        int k = stripe_info.k;
        int r = stripe_info.r;
        int g_m = stripe_info.g_m;
        ECProject::EncodeType encode_type = m_encode_parameters.encodetype;
        ECProject::PlacementType s_placement_type = m_encode_parameters.s_placementtype;
        int l = (k + g_m + r - 1) / r;
        if(encode_type == Azu_LRC)
        {
            l = (k + r - 1) / r;
        }
        
        // generate stripe information
        std::map<int, int> block2group;
        std::vector<std::vector<int>> stripe_groups;
        if(encode_type == U_LRC)
        {
            generate_stripe_information_for_U_LRC(k, r, g_m, stripe_groups, block2group);
        }
        else if(encode_type == NU_LRC)
        {
            if(testtype)
            {
                generate_stripe_information_for_NU_LRC(k, g_m, stripe_groups, block2group, stripe_info.cp);
            }
            else
            {
                generate_stripe_information_for_NU_LRC(k, g_m, stripe_groups, block2group, m_encode_parameters.cp);
            }
        }
        else
        {
            generate_stripe_information_for_Azu_LRC(k, r, g_m, stripe_groups, block2group);
        }
        
        // check
        if(l != int(stripe_groups.size()) && (l != int(stripe_groups.size()) - 1 && encode_type == Azu_LRC))
        {
            std::cout << "Error! Local groups number not matches!" << std::endl;
        }

        if(IF_DEBUG)
        {
            std::cout << "Stripe information: " << std::endl;
            for(int i = 0; i < int(stripe_groups.size()); i++)
            {
                std::cout << "Group " << i << ": ";
                for(int j = 0; j < int(stripe_groups[i].size()); j++)
                {
                    std::cout << stripe_groups[i][j] << " ";
                }
                std::cout << std::endl;
            }
        }
        

        int object_idx = 0;
        std::string object_key = stripe_info.object_keys[object_idx];
        int object_len = stripe_info.object_sizes[object_idx] / block_size;
        Block *blocks_info = new Block[k + g_m + l];
        for(int i = 0; i < k + g_m + l; i++)
        {
            blocks_info[i].block_id = i;
            blocks_info[i].block_size = block_size;
            blocks_info[i].map2group = block2group[i];
            blocks_info[i].map2stripe = stripe_id;
            if (i < k)
            {
                std::string tmp = "_D";
                if (i < 10)
                    tmp = "_D0";
                blocks_info[i].block_key = "S" + std::to_string(stripe_id) + tmp + std::to_string(i);
                blocks_info[i].map2key = object_key;
            }
            else if (i >= k && i < k + g_m)
            {
                blocks_info[i].block_key = "S" + std::to_string(stripe_id) + "_G" + std::to_string(i - k);
                blocks_info[i].map2key = "";
            }
            else
            {
                blocks_info[i].block_key = "S" + std::to_string(stripe_id) + "_L" + std::to_string(i - k - g_m);
                blocks_info[i].map2key = "";
            }
            stripe_info.blocks.push_back(&blocks_info[i]);

            object_len--;
            if(object_len == 0 && object_idx < int(stripe_info.object_keys.size() - 1))
            {
                object_idx++;
                object_key = stripe_info.object_keys[object_idx];
                object_len = stripe_info.object_sizes[object_idx] / block_size;
            }
        }

        if(s_placement_type == Opt)
        {
            if(encode_type == U_LRC || encode_type == NU_LRC)
            {
                std::vector<int> free_clusters;
                for(int i = 0; i < m_num_of_Clusters; i++)
                {
                    free_clusters.push_back(i);
                }
                // for each group
                for(int i = 0; i < l; i++)
                {
                    std::vector<int> local_group = stripe_groups[i];
                    int group_size = int(local_group.size());
                    // place every g + 1 blocks in a unique cluster
                    for(int j = 0; j < group_size; j += g_m + 1)
                    {
                        int upperbound = j + g_m + 1;
                        if(j + g_m + 1 > group_size)
                            upperbound = group_size;
                        // randomly select a cluster
                        if(int(free_clusters.size()) == 0)
                        {
                            std::cout << "No free cluster!" << std::endl;
                            exit(0);
                        }
                        int ran_c_idx = rand_num(int(free_clusters.size()));
                        int t_cluster_id = free_clusters[ran_c_idx];
                        Cluster &t_cluster = m_cluster_table[t_cluster_id];
                        std::vector<int> free_nodes;
                        for(int ii = t_cluster_id * m_num_of_Nodes_in_Cluster; ii < (t_cluster_id + 1) * m_num_of_Nodes_in_Cluster; ii++)
                        {
                            free_nodes.push_back(ii);
                        }

                        for(int jj = j; jj < upperbound; jj++)
                        {
                            // randomly select a node in cluster
                            int ran_n_idx = rand_num(int(free_nodes.size()));
                            int t_node_id = free_nodes[ran_n_idx];

                            int block_id = local_group[jj];
                            blocks_info[block_id].map2cluster = t_cluster_id;
                            blocks_info[block_id].map2node = t_node_id;
                            update_stripe_info_in_node(true, t_node_id, stripe_id);
                            t_cluster.blocks.push_back(&blocks_info[block_id]);
                            t_cluster.stripes.insert(stripe_id);
                            stripe_info.place2clusters.insert(t_cluster_id);

                            auto it_n = std::find(free_nodes.begin(), free_nodes.end(), t_node_id);
                            if(it_n != free_nodes.end())
                            {
                                free_nodes.erase(it_n);
                            }
                        }

                        auto it_c = std::find(free_clusters.begin(), free_clusters.end(), t_cluster_id);
                        if(it_c != free_clusters.end())
                        {
                            free_clusters.erase(it_c);
                        }
                    }
                }
            }
            else if(encode_type == Azu_LRC)
            {
                int theta = l;
                if(r % (g_m + 1) != 0)
                {
                    theta = g_m / (r % (g_m + 1));
                }
                std::vector<std::vector<int>> remain_groups;
                std::map<int, int> c2dg;
                std::map<int, std::vector<int>> c2fn;
                std::vector<int> free_clusters;
                for(int i = 0; i < m_num_of_Clusters; i++)
                {
                    free_clusters.push_back(i);
                }
                for(int i = 0; i < l; i++) // place l local group
                {
                    std::vector<int> local_group = stripe_groups[i];
                    int group_size = int(local_group.size());
                    // place every g + 1 blocks in a unique cluster
                    for(int j = 0; j < group_size; j += g_m + 1)
                    {
                        if(j + g_m + 1 > group_size) // derive the remain group
                        {
                            std::vector<int> remain_group;
                            for(int ii = j; ii < group_size; ii++)
                            {
                                remain_group.push_back(local_group[ii]);
                            }
                            remain_groups.push_back(remain_group);
                            break;
                        }

                        // randomly select a cluster
                        if(int(free_clusters.size()) == 0)
                        {
                            std::cout << "No free cluster!" << std::endl;
                            exit(0);
                        }
                        int data_global_blocks_in_cluster_cnt = 0;
                        int ran_c_idx = rand_num(int(free_clusters.size()));
                        int t_cluster_id = free_clusters[ran_c_idx];
                        Cluster &t_cluster = m_cluster_table[t_cluster_id];
                        std::vector<int> free_nodes;
                        for(int ii = t_cluster_id * m_num_of_Nodes_in_Cluster; ii < (t_cluster_id + 1) * m_num_of_Nodes_in_Cluster; ii++)
                        {
                            free_nodes.push_back(ii);
                        }

                        for(int jj = j; jj < j + g_m + 1; jj++)
                        {
                            // randomly select a node in cluster
                            int ran_n_idx = rand_num(int(free_nodes.size()));
                            int t_node_id = free_nodes[ran_n_idx];

                            int block_id = local_group[jj];
                            if(block_id < k + g_m)
                            {
                                data_global_blocks_in_cluster_cnt += 1;
                            }
                            blocks_info[block_id].map2cluster = t_cluster_id;
                            blocks_info[block_id].map2node = t_node_id;
                            update_stripe_info_in_node(true, t_node_id, stripe_id);
                            t_cluster.blocks.push_back(&blocks_info[block_id]);
                            t_cluster.stripes.insert(stripe_id);
                            stripe_info.place2clusters.insert(t_cluster_id);

                            auto it_n = std::find(free_nodes.begin(), free_nodes.end(), t_node_id);
                            if(it_n != free_nodes.end())
                            {
                                free_nodes.erase(it_n);
                            }
                        }

                        auto it_c = std::find(free_clusters.begin(), free_clusters.end(), t_cluster_id);
                        if(it_c != free_clusters.end())
                        {
                            free_clusters.erase(it_c);
                        }

                        if(data_global_blocks_in_cluster_cnt < g_m)
                        {
                            c2fn[t_cluster_id] = free_nodes;
                            c2dg[t_cluster_id] = data_global_blocks_in_cluster_cnt;
                        }
                    }
                }
                
                int remain_groups_num = int(remain_groups.size());
                for(int i = 0; i < remain_groups_num; i += theta) // place every θ remain groups
                {
                    if(int(free_clusters.size()) == 0)
                    {
                        std::cout << "No free cluster!" << std::endl;
                        exit(0);
                    }
                    // randomly select a cluster
                    int data_global_blocks_in_cluster_cnt = 0;
                    int ran_c_idx = rand_num(int(free_clusters.size()));
                    int t_cluster_id = free_clusters[ran_c_idx];
                    Cluster &t_cluster = m_cluster_table[t_cluster_id];
                    std::vector<int> free_nodes;
                    for(int ii = t_cluster_id * m_num_of_Nodes_in_Cluster; ii < (t_cluster_id + 1) * m_num_of_Nodes_in_Cluster; ii++)
                    {
                        free_nodes.push_back(ii);
                    }
                    for(int j = i; j < i + theta && j < remain_groups_num; j++)
                    {
                        std::vector<int> remain_group = remain_groups[j];
                        int remain_block_num = int(remain_group.size());
                        for(int jj = 0; jj < remain_block_num; jj++)
                        {
                            // randomly select a node in cluster
                            int ran_n_idx = rand_num(int(free_nodes.size()));
                            int t_node_id = free_nodes[ran_n_idx];

                            int block_id = remain_group[jj];
                            if(block_id < k + g_m)
                            {
                                data_global_blocks_in_cluster_cnt += 1;
                            }
                            blocks_info[block_id].map2cluster = t_cluster_id;
                            blocks_info[block_id].map2node = t_node_id;
                            update_stripe_info_in_node(true, t_node_id, stripe_id);
                            t_cluster.blocks.push_back(&blocks_info[block_id]);
                            t_cluster.stripes.insert(stripe_id);
                            stripe_info.place2clusters.insert(t_cluster_id);

                            auto it_n = std::find(free_nodes.begin(), free_nodes.end(), t_node_id);
                            if(it_n != free_nodes.end())
                            {
                                free_nodes.erase(it_n);
                            }
                        }
                    }
                    auto it_c = std::find(free_clusters.begin(), free_clusters.end(), t_cluster_id);
                    if(it_c != free_clusters.end())
                    {
                        free_clusters.erase(it_c);
                    }

                    if(data_global_blocks_in_cluster_cnt < g_m)
                    {
                        c2fn[t_cluster_id] = free_nodes;
                        c2dg[t_cluster_id] = data_global_blocks_in_cluster_cnt;
                    }
                }
                // place global parities
                std::vector<int> global_group = stripe_groups[l];
                int global_idx = 0;
                for(auto it = c2dg.begin(); it != c2dg.end() && global_idx < g_m; it++)
                {
                    int t_cluster_id = it->first;
                    int space = g_m - it->second;
                    Cluster &t_cluster = m_cluster_table[t_cluster_id];
                    std::vector<int> free_nodes = c2fn[t_cluster_id];
                    for(int i = 0; i < space && global_idx < g_m; i++)
                    {
                        // randomly select a node in cluster
                        int ran_n_idx = rand_num(int(free_nodes.size()));
                        int t_node_id = free_nodes[ran_n_idx];

                        int block_id = global_group[global_idx++];
                        blocks_info[block_id].map2cluster = t_cluster_id;
                        blocks_info[block_id].map2node = t_node_id;
                        update_stripe_info_in_node(true, t_node_id, stripe_id);
                        t_cluster.blocks.push_back(&blocks_info[block_id]);
                        t_cluster.stripes.insert(stripe_id);
                        stripe_info.place2clusters.insert(t_cluster_id);

                        auto it_n = std::find(free_nodes.begin(), free_nodes.end(), t_node_id);
                        if(it_n != free_nodes.end())
                        {
                            free_nodes.erase(it_n);
                        }
                    }
                }
                if(global_idx < g_m)
                {
                    if(int(free_clusters.size()) == 0)
                    {
                        std::cout << "No free cluster!" << std::endl;
                        exit(0);
                    }
                    int ran_c_idx = rand_num(int(free_clusters.size()));
                    int t_cluster_id = free_clusters[ran_c_idx];
                    Cluster &t_cluster = m_cluster_table[t_cluster_id];
                    std::vector<int> free_nodes;
                    for(int ii = t_cluster_id * m_num_of_Nodes_in_Cluster; ii < (t_cluster_id + 1) * m_num_of_Nodes_in_Cluster; ii++)
                    {
                        free_nodes.push_back(ii);
                    }
                    for(int i = global_idx; i < g_m; i++)
                    {
                        // randomly select a node in cluster
                        int ran_n_idx = rand_num(int(free_nodes.size()));
                        int t_node_id = free_nodes[ran_n_idx];

                        int block_id = global_group[global_idx++];
                        blocks_info[block_id].map2cluster = t_cluster_id;
                        blocks_info[block_id].map2node = t_node_id;
                        update_stripe_info_in_node(true, t_node_id, stripe_id);
                        t_cluster.blocks.push_back(&blocks_info[block_id]);
                        t_cluster.stripes.insert(stripe_id);
                        stripe_info.place2clusters.insert(t_cluster_id);

                        auto it_n = std::find(free_nodes.begin(), free_nodes.end(), t_node_id);
                        if(it_n != free_nodes.end())
                        {
                            free_nodes.erase(it_n);
                        }
                    }
                    auto it_c = std::find(free_clusters.begin(), free_clusters.end(), t_cluster_id);
                    if(it_c != free_clusters.end())
                    {
                        free_clusters.erase(it_c);
                    }
                }
            }
        }
        else if(s_placement_type == Flat)
        {
            // std::vector<int> free_nodes;
            // for(int i = 0; i < m_num_of_Clusters * m_num_of_Nodes_in_Cluster; i++)
            // {
            //     free_nodes.push_back(i);
            // }
            // for each block
            for(int i = 0; i < k + g_m + l; i++)
            {
                // randomly select a node
                // int ran_n_idx = rand_num(int(free_nodes.size()));
                // int t_node_id = free_nodes[ran_n_idx];
                int t_node_id = i;
                int t_cluster_id = m_node_table[t_node_id].map2cluster;
                Cluster &t_cluster = m_cluster_table[t_cluster_id];
                int block_id = i;
                blocks_info[block_id].map2cluster = t_cluster_id;
                blocks_info[block_id].map2node = t_node_id;
                update_stripe_info_in_node(true, t_node_id, stripe_id);
                t_cluster.blocks.push_back(&blocks_info[block_id]);
                t_cluster.stripes.insert(stripe_id);
                stripe_info.place2clusters.insert(t_cluster_id);

                // auto it_n = std::find(free_nodes.begin(), free_nodes.end(), t_node_id);
                // if(it_n != free_nodes.end())
                // {
                //     free_nodes.erase(it_n);
                // }
            }
        }
        else if(s_placement_type == Ran)
        {
            std::vector<int> free_clusters;
            std::vector<int> cluster_blocks;
            for(int i = 0; i < m_num_of_Clusters; i++)
            {
                free_clusters.push_back(i);
                cluster_blocks.push_back(g_m + 1);
            }
            // for each block
            for(int i = 0; i < k + g_m + l; i++)
            {
                // randomly select a cluster
                if(int(free_clusters.size()) == 0)
                {
                    std::cout << "No free cluster!" << std::endl;
                    exit(0);
                }
                int ran_c_idx = rand_num(int(free_clusters.size()));
                int t_cluster_id = free_clusters[ran_c_idx];
                Cluster &t_cluster = m_cluster_table[t_cluster_id];
                cluster_blocks[ran_c_idx] -= 1;
                // randomly select a node in cluster
                int t_node_id = rand_num_in_range(t_cluster_id * m_num_of_Nodes_in_Cluster, (t_cluster_id + 1) * m_num_of_Nodes_in_Cluster - 1);
                
                int block_id = i;
                blocks_info[block_id].map2cluster = t_cluster_id;
                blocks_info[block_id].map2node = t_node_id;
                update_stripe_info_in_node(true, t_node_id, stripe_id);
                t_cluster.blocks.push_back(&blocks_info[block_id]);
                t_cluster.stripes.insert(stripe_id);
                stripe_info.place2clusters.insert(t_cluster_id);

                if(cluster_blocks[ran_c_idx] == 0)
                {
                    auto it_c = std::find(free_clusters.begin(), free_clusters.end(), t_cluster_id);
                    if(it_c != free_clusters.end())
                    {
                        free_clusters.erase(it_c);
                    }
                    auto it_b = std::find(cluster_blocks.begin(), cluster_blocks.end(), 0);
                    if(it_b != cluster_blocks.end())
                    {
                        cluster_blocks.erase(it_b);
                    }
                }
                
            }
        }

        if (IF_DEBUG)
        {
            std::cout << std::endl;
            std::cout << "Data placement result:" << std::endl;
            for (int i = 0; i < m_num_of_Clusters; i++)
            {
                Cluster &t_cluster = m_cluster_table[i];
                if (int(t_cluster.blocks.size()) > 0)
                {
                    std::cout << "Cluster " << i << ": ";
                    for (auto it = t_cluster.blocks.begin(); it != t_cluster.blocks.end(); it++)
                    {
                        std::cout << "[" << (*it)->block_key << ":S" << (*it)->map2stripe << "G" << (*it)->map2group << "N" << (*it)->map2node << "] ";
                    }
                    std::cout << std::endl;
                }
            }
        }

        // randomly select a cluster
        int r_idx = rand_num(int(stripe_info.place2clusters.size()));
        int selected_cluster_id = *(std::next(stripe_info.place2clusters.begin(), r_idx));
        if (IF_DEBUG)
        {
            std::cout << "[SET] Select the proxy in cluster " << selected_cluster_id << " to encode and set!" << std::endl;
        }
        return selected_cluster_id;
    }
} // namespace ECProject