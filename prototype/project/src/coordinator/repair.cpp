#include "coordinator.h"
#include "tinyxml2.h"
#include <random>
#include <algorithm>
#include <unistd.h>
#include <sys/time.h>

namespace ECProject
{
  void CoordinatorImpl::request_repair(bool isblock, int stripe_id, std::shared_ptr<std::vector<int>> blocks_or_nodes, coordinator_proto::RepIfRepaired *repairReplyClient)
  {
    bool if_partial_decoding = m_encode_parameters.partial_decoding;
    // ECProject::EncodeType encodetype = m_encode_parameters.encodetype;
    auto failure_map = std::make_shared<std::map<int, std::vector<Block *>>>();
    auto failures_type = std::make_shared<std::map<int, ECProject::FailureType>>();

    check_out_failures(isblock, stripe_id, blocks_or_nodes, failure_map, failures_type);

    double t_rc = 0.0;
    double temp_time = 0.0;
    struct timeval r_start_time, r_end_time;
    int tot_stripe_num = int(failure_map->size());
    int stripe_cnt = 0;
    int failed_cnt = 0;
    // for simulation
    int t_cross_cluster = 0;

    for(auto it1 = failure_map->begin(); it1 != failure_map->end(); it1++)
    {
        int t_stripe_id = it1->first;
        failed_cnt += int((it1->second).size());
        auto it2 = failures_type->find(t_stripe_id);
        ECProject::FailureType ft = it2->second;
        if(IF_DEBUG)
        {
            std::cout << std::endl;
            std::cout << "[Type " << ft << "] Failed Stripe " << t_stripe_id  << " (f" << (it1->second).size() << ") : ";
            for(auto t_it = (it1->second).begin(); t_it != (it1->second).end(); t_it++)
            {
                std::cout << (*t_it)->block_key << " ";
            }
            std::cout << std::endl;
        }
        std::vector<proxy_proto::mainRepairPlan> main_repair;
        std::vector<std::vector<proxy_proto::helpRepairPlan>> help_repair;
        bool flag = true;
        if(ft == Single_Block)
        {
            flag = generate_repair_plan_for_single_block_LRC(main_repair, help_repair, t_stripe_id, it1->second);
        }
        else
        {
            flag = generate_repair_plan_for_multi_blocks_LRC(main_repair, help_repair, t_stripe_id, it1->second);
        }

        auto send_main_repair_plan = [this, main_repair](int i, int m_cluster_id) mutable
        {
            grpc::ClientContext context_m;
            proxy_proto::RepairReply response_m;
            std::string chosen_proxy_m = m_cluster_table[m_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[m_cluster_id].proxy_port);
            grpc::Status stat1 = m_proxy_ptrs[chosen_proxy_m]->mainRepair(&context_m, main_repair[i], &response_m);
            if (IF_DEBUG)
            {
                std::cout << "Selected main proxy " << chosen_proxy_m << " of Cluster" << m_cluster_id << std::endl;
            }
        };

        auto send_help_repair_plan = [this, main_repair, help_repair](int i, int j, std::string proxy_ip, int proxy_port) mutable
        {
            grpc::ClientContext context_h;
            proxy_proto::RepairReply response_h;
            std::string chosen_proxy_h = proxy_ip + ":" + std::to_string(proxy_port);
            grpc::Status stat1 = m_proxy_ptrs[chosen_proxy_h]->helpRepair(&context_h, help_repair[i][j], &response_h);
            if (IF_DEBUG)
            {
                std::cout << "Selected help proxy " << chosen_proxy_h << std::endl;
            }
        };

        // simulation
        if(flag)
        {
            simulation_repair(main_repair, t_cross_cluster);
            for(int i = 0; i < int(main_repair.size()); i++)
            {
                if(IF_DEBUG)
                {
                    std::cout << "> Failed Blocks: ";
                    for(int j = 0; j < int(main_repair[i].failed_blockkeys_size()); j++)
                    {
                        std::cout << main_repair[i].failed_blockkeys(j) << " ";
                    }
                    std::cout << std::endl;
                    std::cout << "> Repair by Blocks: ";
                    for(int j = 0; j < int(main_repair[i].clusters_size()); j++)
                    {
                        std::cout << "[Cluster" << main_repair[i].clusters(j).cluster_id() << "][" << int(main_repair[i].clusters(j).blockkeys_size()) << "]";
                        for(int jj = 0; jj < int(main_repair[i].clusters(j).blockkeys_size()); jj++)
                        {
                            std::cout << main_repair[i].clusters(j).blockkeys(jj) << " ";
                        }
                    }
                    std::cout << std::endl;
                }
            }
        }

        temp_time = 0.0;
        gettimeofday(&r_start_time, NULL);
        if(!flag)
        {
            std::cout << "Undecodable!" << std::endl;
        }
        else
        {
            for(int i = 0; i < int(main_repair.size()); i++)
            {
                try
                {
                    int failed_block_num = main_repair[i].failed_blockids_size();
                    int m_cluster_id = main_repair[i].s_cluster_id();
                    std::thread my_main_thread(send_main_repair_plan, i, m_cluster_id);
                    std::vector<std::thread> senders;
                    int index = 0;
                    for(int j = 0; j < int(main_repair[i].clusters_size()); j++)
                    {
                        int t_cluster_id = main_repair[i].clusters(j).cluster_id();
                        int t_datablock_num = main_repair[i].clusters(j).blockids_size();
                        if(IF_DEBUG)
                        {
                            std::cout << int(help_repair[i].size()) << "|" << m_cluster_id << "|" << t_cluster_id << " ";
                        }
                        if(t_cluster_id != m_cluster_id && if_partial_decoding && failed_block_num < t_datablock_num)
                        {
                            std::string proxy_ip = main_repair[i].clusters(j).proxy_ip();
                            int proxy_port = main_repair[i].clusters(j).proxy_port();
                            if(IF_DEBUG)
                            {
                                std::cout << proxy_ip << ":" << proxy_port << " ";
                            }
                            // if not partail_decoding or partail_blocks_num >= data_blocks_num, don't send
                            senders.push_back(std::thread(send_help_repair_plan, i, index, proxy_ip, proxy_port));
                        }
                        if(t_cluster_id != m_cluster_id)
                        {
                            index++;
                        }
                    }
                    if(IF_DEBUG)
                    {
                        std::cout << std::endl;
                    }
                    for (int j = 0; j < int(senders.size()); j++)
                    {
                        senders[j].join();
                    }
                    my_main_thread.join();

                    // check
                    proxy_proto::AskIfSuccess ask_c0;
                    ask_c0.set_step(0);
                    grpc::ClientContext context_c0;
                    proxy_proto::RepIfSuccess response_c0;
                    std::string chosen_proxy_c0 = m_cluster_table[m_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[m_cluster_id].proxy_port);
                    grpc::Status stat_c0 = m_proxy_ptrs[chosen_proxy_c0]->checkStep(&context_c0, ask_c0, &response_c0);
                    if (stat_c0.ok() && response_c0.ifsuccess() && IF_DEBUG)
                    {
                        std::cout << "[Repair] block repair success in Cluster" << m_cluster_id << std::endl;
                    }
                    else if(IF_DEBUG)
                    {
                        std::cout << "\033[1;37m[Repair] Failed here!!! In Cluster" << m_cluster_id << "\033[0m" << std::endl;
                        exit(0);
                    }
                }
                catch(const std::exception& e)
                {
                    std::cerr << e.what() << '\n';
                }
            }
        }
        gettimeofday(&r_end_time, NULL);
        temp_time = r_end_time.tv_sec - r_start_time.tv_sec + (r_end_time.tv_usec - r_start_time.tv_usec) * 1.0 / 1000000;
        t_rc += temp_time;

        stripe_cnt++;
        if(IF_DEBUG)
        {
            std::cout << "[Repair] Process " << stripe_cnt << "/" << tot_stripe_num << "  rc:" << t_rc << std::endl;
        }
    }
    if(IF_DEBUG)
    {
        std::cout << "[Failures] " << failed_cnt << std::endl << std::endl;
    }
    repairReplyClient->set_ifrepaired(true);
    repairReplyClient->set_rc(t_rc);
    repairReplyClient->set_failed_stripe_num(failed_cnt);
    repairReplyClient->set_cross_cluster_num(t_cross_cluster);
  }

    void CoordinatorImpl::check_out_failures(
        bool isblock, int stripe_id, std::shared_ptr<std::vector<int>> failureinfo, 
        std::shared_ptr<std::map<int, std::vector<Block *>>> failure_map,
        std::shared_ptr<std::map<int, ECProject::FailureType>> failures_type)
    {
        if(isblock)
        {
            Stripe &t_stripe = m_stripe_table[stripe_id];
            std::vector<Block *> t_blocks_list;
            for(int i = 0; i < int(failureinfo->size()); i++)
            {
                int t_block_id = (*failureinfo)[i];
                for(int j = 0; j < int(t_stripe.blocks.size()); j++)
                {
                if(t_stripe.blocks[j]->block_id == t_block_id)
                {
                    t_blocks_list.push_back(t_stripe.blocks[j]);
                    break;
                }
                }
            }
            (*failure_map)[stripe_id] = t_blocks_list;
            if(int(failureinfo->size()) == 1)
            {
                (*failures_type)[stripe_id] = ECProject::Single_Block;
            }
            else
            {
                (*failures_type)[stripe_id] = ECProject::Multi_Blocks;
            }
        }
        else
        {
            for(int i = 0; i < int(failureinfo->size()); i++)
            {
                int t_node_id = (*failureinfo)[i];
                int t_cluster_id = m_node_table[t_node_id].map2cluster;
                Cluster &t_cluster = m_cluster_table[t_cluster_id];
                for(int j = 0; j < int(t_cluster.blocks.size()); j++)
                {
                    if(t_cluster.blocks[j]->map2node == t_node_id)
                    {
                        int t_stripe_id = t_cluster.blocks[j]->map2stripe;
                        auto it = failure_map->find(t_stripe_id);
                        if(it == failure_map->end())
                        {
                            std::vector<Block *> tmp;
                            tmp.push_back(t_cluster.blocks[j]);
                            failure_map->insert(std::make_pair(t_stripe_id, tmp));
                        }
                        else
                        {
                            (it->second).push_back(t_cluster.blocks[j]);
                        }
                    }
                }
            }
            // if(IF_DEBUG)
            // {
            //   std::cout << std::endl << "Failure Map:" << std::endl;
            //   for(auto it = failure_map->begin(); it != failure_map->end(); it++)
            //   {
            //     int t_stripe_id = it->first;
            //     std::cout << "Stripe " << t_stripe_id << ": ";
            //     std::vector<Block *> &failed_list = it->second;
            //     for(int j = 0; j < int(failed_list.size()); j++)
            //     {
            //       std::cout << failed_list[j]->block_key << " ";
            //     }
            //     std::cout << std::endl;
            //   }
            // }
            for(auto it = failure_map->begin(); it != failure_map->end(); it++)
            {
                int t_stripe_id = it->first;
                if(int((it->second).size()) == 1)
                {
                    (*failures_type)[t_stripe_id] = ECProject::Single_Block;
                }
                else
                {
                    (*failures_type)[t_stripe_id] = ECProject::Multi_Blocks;
                }
            }
        }
    }

    bool CoordinatorImpl::generate_repair_plan_for_single_block_LRC(
        std::vector<proxy_proto::mainRepairPlan> &main_repair, 
        std::vector<std::vector<proxy_proto::helpRepairPlan>> &help_repair, 
        int stripe_id, std::vector<Block *> &failed_blocks)
    {
        Stripe &t_stripe = m_stripe_table[stripe_id];
		int k = t_stripe.k;
        int g_m = t_stripe.g_m;
        ECProject::EncodeType encode_type = m_encode_parameters.encodetype;
        int l = (k + g_m + t_stripe.r - 1) / t_stripe.r;
        if(encode_type == Azu_LRC)
        {
            l = (k + t_stripe.r - 1) / t_stripe.r;
        }
        Block *t_block_ptr = failed_blocks[0];
        int block_size = t_block_ptr->block_size;
        int t_map2cluster = t_block_ptr->map2cluster;
        int t_map2group = t_block_ptr->map2group;
        int t_blockid = t_block_ptr->block_id;
        proxy_proto::mainRepairPlan t_main_plan;

        std::vector<Block *> blocks_map(k + g_m + l, nullptr);
        for(int i = 0; i < int(t_stripe.blocks.size()); i++)
        {
            int t_block_id = t_stripe.blocks[i]->block_id;
            blocks_map[t_block_id] = t_stripe.blocks[i];
        }

        int stop_idx = 0;
        std::unordered_set<int> t_cluster_set;
        if(t_blockid >= k && t_blockid < k + g_m && encode_type == Azu_LRC) // global parity repaired by k data blocks (Azure-LRC)
        {
            t_main_plan.set_is_local_repair(false);
            int cnt = 0;
            for(int i = 0; i < k + g_m; i++)
            {
                if(i != t_blockid && cnt < k)
                {
                    t_main_plan.add_surviving_blockids(blocks_map[i]->block_id);
                    t_cluster_set.insert(blocks_map[i]->map2cluster);
                    cnt++;
                    if(cnt == k)
                    {
                        stop_idx = i;
                        break;
                    }
                }
            }
        }
        else // other, repair locally
        {
            t_main_plan.set_is_local_repair(true);
            for(int i = 0; i < int(blocks_map.size()); i++) 
            {
                if(blocks_map[i]->map2group == t_map2group && i != t_blockid)
                {
                    t_cluster_set.insert(blocks_map[i]->map2cluster);
                }
            }
        }
    
        std::vector<proxy_proto::helpRepairPlan> t_help_plans;
        for(auto it = t_cluster_set.begin(); it != t_cluster_set.end(); it++)
        {
            int t_cluster_id = *it;
            proxy_proto::helpRepairPlan t_help_plan;
            auto blocks_location = t_main_plan.add_clusters();
            blocks_location->set_cluster_id(t_cluster_id);
            t_help_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
            t_help_plan.set_mainproxyip(m_cluster_table[t_map2cluster].proxy_ip);
            t_help_plan.set_mainproxyport(m_cluster_table[t_map2cluster].proxy_port + 1);
            t_help_plan.set_k(k);
            t_help_plan.set_g_m(g_m);
            t_help_plan.set_l(l);
            t_help_plan.set_block_size(block_size);
            t_help_plan.set_encodetype(encode_type);
            t_help_plan.add_failed_blockids(t_block_ptr->block_id);
            if(t_blockid >= k && t_blockid < k + g_m && encode_type == Azu_LRC) // global parity repaired by k data blocks (Azure-LRC)
            {
                for(int i = 0; i < int(t_main_plan.surviving_blockids_size()); i++)
                {
                    t_help_plan.add_surviving_blockids(t_main_plan.surviving_blockids(i));
                }
                t_help_plan.set_is_local_repair(false);
                for(int i = 0; i < stop_idx; i++)
                {
                    if(t_cluster_id == blocks_map[i]->map2cluster && i != t_blockid)
                    {
                        int t_node_id = blocks_map[i]->map2node;
                        t_help_plan.add_blockkeys(blocks_map[i]->block_key);
                        t_help_plan.add_blockids(blocks_map[i]->block_id);
                        t_help_plan.add_datanodeip(m_node_table[t_node_id].node_ip);
                        t_help_plan.add_datanodeport(m_node_table[t_node_id].node_port);

                        blocks_location->add_blockkeys(blocks_map[i]->block_key);
                        blocks_location->add_blockids(blocks_map[i]->block_id);
                        blocks_location->add_datanodeip(m_node_table[t_node_id].node_ip);
                        blocks_location->add_datanodeport(m_node_table[t_node_id].node_port);
                    }
                }
            }
            else
            {
                t_help_plan.set_is_local_repair(true);
                for(int i = 0; i < int(blocks_map.size()); i++)
                {
                    if(t_cluster_id == blocks_map[i]->map2cluster && blocks_map[i]->map2group == t_map2group && i != t_blockid)
                    {
                        int t_node_id = blocks_map[i]->map2node;
                        t_help_plan.add_blockkeys(blocks_map[i]->block_key);
                        t_help_plan.add_blockids(blocks_map[i]->block_id);
                        t_help_plan.add_datanodeip(m_node_table[t_node_id].node_ip);
                        t_help_plan.add_datanodeport(m_node_table[t_node_id].node_port);

                        blocks_location->add_blockkeys(blocks_map[i]->block_key);
                        blocks_location->add_blockids(blocks_map[i]->block_id);
                        blocks_location->add_datanodeip(m_node_table[t_node_id].node_ip);
                        blocks_location->add_datanodeport(m_node_table[t_node_id].node_port);
                    }
                }
            }
            if(t_cluster_id != t_map2cluster)
                t_help_plans.push_back(t_help_plan);

            blocks_location->set_proxy_ip(m_cluster_table[t_cluster_id].proxy_ip);
            blocks_location->set_proxy_port(m_cluster_table[t_cluster_id].proxy_port);
        }
        t_main_plan.add_failed_blockkeys(t_block_ptr->block_key);
        t_main_plan.add_failed_blockids(t_block_ptr->block_id);
        t_main_plan.add_failed_datanodeip(m_node_table[t_block_ptr->map2node].node_ip);
        t_main_plan.add_failed_datanodeport(m_node_table[t_block_ptr->map2node].node_port);
        t_main_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
        t_main_plan.set_k(k);
        t_main_plan.set_g_m(g_m);
        t_main_plan.set_l(l);
        t_main_plan.set_block_size(block_size);
        t_main_plan.set_encodetype(encode_type);
        t_main_plan.set_s_cluster_id(t_map2cluster);
        main_repair.push_back(t_main_plan);
        help_repair.push_back(t_help_plans);
        return true;
    }

    bool CoordinatorImpl::generate_repair_plan_for_multi_blocks_LRC(
        std::vector<proxy_proto::mainRepairPlan> &main_repair, 
        std::vector<std::vector<proxy_proto::helpRepairPlan>> &help_repair, 
        int stripe_id, std::vector<Block *> &failed_blocks)
    {
        Stripe &t_stripe = m_stripe_table[stripe_id];
        int k = t_stripe.k;
        int g_m = t_stripe.g_m;
        ECProject::EncodeType encode_type = m_encode_parameters.encodetype;
        int l = (k + g_m + t_stripe.r - 1) / t_stripe.r;
        if(encode_type == Azu_LRC)
        {
            l = (k + t_stripe.r - 1) / t_stripe.r;
        }

        std::vector<int> failed_map(k + g_m + l, 0);
        std::vector<Block *> blocks_map(k + g_m + l, nullptr);
        std::vector<int> fb_group_cnt(l, 0);
        int data_or_global_failed_num = 0;
        int failed_blocks_num = int(failed_blocks.size());
        for(int i = 0; i < failed_blocks_num; i++)
        {
            int block_id = failed_blocks[i]->block_id;
            int map2group = failed_blocks[i]->map2group;
            failed_map[block_id] = 1;
            fb_group_cnt[map2group] += 1;
            if(block_id < k + g_m)
            {
                data_or_global_failed_num += 1;
            }
        }
        for(int i = 0; i < int(t_stripe.blocks.size()); i++)
        {
            int t_block_id = t_stripe.blocks[i]->block_id;
            blocks_map[t_block_id] = t_stripe.blocks[i];
        }
    
        int iter_cnt = 0;
        while(failed_blocks_num > 0)
        {
            // repair in local group
            for(int group_id = 0; group_id < l; group_id++)
            {
                if(fb_group_cnt[group_id] == 1)
                {
                    proxy_proto::mainRepairPlan t_main_plan;
                    std::unordered_set<int> t_cluster_set;
                    Block *t_block_ptr = nullptr;
                    for(int i = 0; i < int(blocks_map.size()); i++)
                    {
                        if(blocks_map[i]->map2group == group_id)
                        {
                            if(failed_map[i])
                            {
                                t_block_ptr = blocks_map[i];
                            }
                            else
                            {
                                t_cluster_set.insert(blocks_map[i]->map2cluster);
                            }
                        } 
                    }

                    int t_blockid = t_block_ptr->block_id;
                    int t_map2cluster = t_block_ptr->map2cluster;
                    int t_map2group = t_block_ptr->map2group;
                    int block_size = t_block_ptr->block_size;
                    std::vector<proxy_proto::helpRepairPlan> t_help_plans;
                    for(auto it = t_cluster_set.begin(); it != t_cluster_set.end(); it++)
                    {
                        int t_cluster_id = *it;
                        proxy_proto::helpRepairPlan t_help_plan;
                        auto blocks_location = t_main_plan.add_clusters();
                        blocks_location->set_cluster_id(t_cluster_id);
                        t_help_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
                        t_help_plan.set_mainproxyip(m_cluster_table[t_map2cluster].proxy_ip);
                        t_help_plan.set_mainproxyport(m_cluster_table[t_map2cluster].proxy_port + 1);
                        t_help_plan.set_k(k);
                        t_help_plan.set_g_m(g_m);
                        t_help_plan.set_l(l);
                        t_help_plan.set_block_size(block_size);
                        t_help_plan.set_encodetype(encode_type);
                        t_help_plan.set_is_local_repair(true);
                        t_help_plan.add_failed_blockids(t_block_ptr->block_id);
                        for(int i = 0; i < int(blocks_map.size()); i++)
                        {
                            if(t_cluster_id == blocks_map[i]->map2cluster && blocks_map[i]->map2group == t_map2group && !failed_map[i])
                            {
                                int t_node_id = blocks_map[i]->map2node;
                                t_help_plan.add_blockkeys(blocks_map[i]->block_key);
                                t_help_plan.add_blockids(blocks_map[i]->block_id);
                                t_help_plan.add_datanodeip(m_node_table[t_node_id].node_ip);
                                t_help_plan.add_datanodeport(m_node_table[t_node_id].node_port);

                                blocks_location->add_blockkeys(blocks_map[i]->block_key);
                                blocks_location->add_blockids(blocks_map[i]->block_id);
                                blocks_location->add_datanodeip(m_node_table[t_node_id].node_ip);
                                blocks_location->add_datanodeport(m_node_table[t_node_id].node_port);
                            }
                        }
                        if(t_cluster_id != t_map2cluster)
                            t_help_plans.push_back(t_help_plan);

                        blocks_location->set_proxy_ip(m_cluster_table[t_cluster_id].proxy_ip);
                        blocks_location->set_proxy_port(m_cluster_table[t_cluster_id].proxy_port);
                    }
                    t_main_plan.add_failed_blockkeys(t_block_ptr->block_key);
                    t_main_plan.add_failed_blockids(t_block_ptr->block_id);
                    t_main_plan.add_failed_datanodeip(m_node_table[t_block_ptr->map2node].node_ip);
                    t_main_plan.add_failed_datanodeport(m_node_table[t_block_ptr->map2node].node_port);
                    t_main_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
                    t_main_plan.set_k(k);
                    t_main_plan.set_g_m(g_m);
                    t_main_plan.set_l(l);
                    t_main_plan.set_block_size(block_size);
                    t_main_plan.set_encodetype(encode_type);
                    t_main_plan.set_is_local_repair(true);
                    t_main_plan.set_s_cluster_id(t_map2cluster);
                    main_repair.push_back(t_main_plan);
                    help_repair.push_back(t_help_plans);
                    // update 
                    failed_map[t_blockid] = 0;
                    fb_group_cnt[group_id] = 0;
                    failed_blocks_num -= 1;
                    if(t_blockid < k + g_m)
                    {
                        data_or_global_failed_num -= 1;
                    }
                }
            }
            // 2 <= data_or_global_failed_num <= g_m
            if(data_or_global_failed_num > 0 && data_or_global_failed_num <= g_m)
            {
                int t_main_cluster_id = -1;
                int block_size = 0;
                int cnt = 0;
                int stop_idx = 0;

                proxy_proto::mainRepairPlan t_main_plan;
                std::unordered_set<int> t_cluster_set;
                for(int i = 0; i < k + g_m; i++)
                {
                    if(failed_map[i])
                    {
                        Block *t_block_ptr = blocks_map[i];
                        t_main_plan.add_failed_blockkeys(t_block_ptr->block_key);
                        t_main_plan.add_failed_blockids(t_block_ptr->block_id);
                        t_main_plan.add_failed_datanodeip(m_node_table[t_block_ptr->map2node].node_ip);
                        t_main_plan.add_failed_datanodeport(m_node_table[t_block_ptr->map2node].node_port);
                        t_main_cluster_id = t_block_ptr->map2cluster;
                        block_size = t_block_ptr->block_size;
                    }
                    else if(cnt < k)    //surviving blocks for repair
                    {
                        t_cluster_set.insert(blocks_map[i]->map2cluster);
                        t_main_plan.add_surviving_blockids(blocks_map[i]->block_id);
                        cnt++;
                        if(cnt == k)
                        {
                            stop_idx = i;
                            break;
                        }
                    }
                }

                std::vector<proxy_proto::helpRepairPlan> t_help_plans;
                for(auto it = t_cluster_set.begin(); it != t_cluster_set.end(); it++)
                {
                    int t_cluster_id = *it;
                    proxy_proto::helpRepairPlan t_help_plan;
                    auto blocks_location = t_main_plan.add_clusters();
                    blocks_location->set_cluster_id(t_cluster_id);
                    t_help_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
                    t_help_plan.set_mainproxyip(m_cluster_table[t_main_cluster_id].proxy_ip);
                    t_help_plan.set_mainproxyport(m_cluster_table[t_main_cluster_id].proxy_port + 1);
                    t_help_plan.set_k(k);
                    t_help_plan.set_g_m(g_m);
                    t_help_plan.set_l(l);
                    t_help_plan.set_block_size(block_size);
                    t_help_plan.set_encodetype(encode_type);
                    t_help_plan.set_is_local_repair(false);
                    for(int i = 0; i < int(t_main_plan.surviving_blockids_size()); i++)
                    {
                        t_help_plan.add_surviving_blockids(t_main_plan.surviving_blockids(i));
                    }
                    for(int i = 0; i < int(t_main_plan.failed_blockids_size()); i++)
                    {
                        t_help_plan.add_failed_blockids(t_main_plan.failed_blockids(i));
                    }
                    for(int i = 0; i <= stop_idx; i++)
                    {
                        if(t_cluster_id == blocks_map[i]->map2cluster && !failed_map[i])
                        {
                            int t_node_id = blocks_map[i]->map2node;
                            t_help_plan.add_blockkeys(blocks_map[i]->block_key);
                            t_help_plan.add_blockids(blocks_map[i]->block_id);
                            t_help_plan.add_datanodeip(m_node_table[t_node_id].node_ip);
                            t_help_plan.add_datanodeport(m_node_table[t_node_id].node_port);

                            blocks_location->add_blockkeys(blocks_map[i]->block_key);
                            blocks_location->add_blockids(blocks_map[i]->block_id);
                            blocks_location->add_datanodeip(m_node_table[t_node_id].node_ip);
                            blocks_location->add_datanodeport(m_node_table[t_node_id].node_port);
                        }
                    }
                    if(t_cluster_id != t_main_cluster_id)
                        t_help_plans.push_back(t_help_plan);

                    blocks_location->set_proxy_ip(m_cluster_table[t_cluster_id].proxy_ip);
                    blocks_location->set_proxy_port(m_cluster_table[t_cluster_id].proxy_port);
                }
                t_main_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
                t_main_plan.set_k(k);
                t_main_plan.set_g_m(g_m);
                t_main_plan.set_l(l);
                t_main_plan.set_block_size(block_size);
                t_main_plan.set_encodetype(encode_type);
                t_main_plan.set_is_local_repair(false);
                t_main_plan.set_s_cluster_id(t_main_cluster_id);
                main_repair.push_back(t_main_plan);
                help_repair.push_back(t_help_plans);
        
                // update 
                for(int i = 0; i < k + g_m; i++)
                {
                    if(failed_map[i])
                    {
                        failed_map[i] = 0;
                        failed_blocks_num -= 1;
                        int group_id = blocks_map[i]->map2group;
                        fb_group_cnt[group_id] -= 1;
                    }
                }
                data_or_global_failed_num = 0;
            }

            if(iter_cnt > 0 && failed_blocks_num > 0)
            {
                auto fls_idx_ptr = std::make_shared<std::vector<int>>();
                for(int i = 0; i < k + g_m + l; i++)
                {
                    if(failed_map[i])
                    {
                        fls_idx_ptr->push_back(i);
                    }
                }
                bool if_decodable = true;
                if(encode_type == U_LRC)
                {
                    if_decodable = check_if_decodable_Uniform_LRC(k, g_m, l, fls_idx_ptr);
                }
                else if(encode_type == NU_LRC)
                {
                    if_decodable = check_if_decodable_Non_Uniform_LRC(k, g_m, l, std::make_shared<std::vector<fp>>(t_stripe.cp), fls_idx_ptr);
                }
                else
                {
                    if_decodable = check_if_decodable_Azu_LRC(k, g_m, l, fls_idx_ptr);
                }
                if(if_decodable)
                {
                    int t_main_cluster_id = -1;
                    int block_size = 0;
                    proxy_proto::mainRepairPlan t_main_plan;
                    std::unordered_set<int> t_cluster_set;
                    for(int i = 0; i < int(blocks_map.size()); i++)
                    {
                        if(failed_map[i])
                        {
                            Block *t_block_ptr = blocks_map[i];
                            t_main_plan.add_failed_blockkeys(t_block_ptr->block_key);
                            t_main_plan.add_failed_blockids(t_block_ptr->block_id);
                            t_main_plan.add_failed_datanodeip(m_node_table[t_block_ptr->map2node].node_ip);
                            t_main_plan.add_failed_datanodeport(m_node_table[t_block_ptr->map2node].node_port);
                            t_main_cluster_id = t_block_ptr->map2cluster;
                            block_size = t_block_ptr->block_size;
                        }
                        else
                        {
                            t_cluster_set.insert(blocks_map[i]->map2cluster);
                            t_main_plan.add_surviving_blockids(blocks_map[i]->block_id);
                        }
                    }
                    std::vector<proxy_proto::helpRepairPlan> t_help_plans;
                    for(auto it = t_cluster_set.begin(); it != t_cluster_set.end(); it++)
                    {
                        int t_cluster_id = *it;
                        proxy_proto::helpRepairPlan t_help_plan;
                        auto blocks_location = t_main_plan.add_clusters();
                        blocks_location->set_cluster_id(t_cluster_id);
                        t_help_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
                        t_help_plan.set_mainproxyip(m_cluster_table[t_main_cluster_id].proxy_ip);
                        t_help_plan.set_mainproxyport(m_cluster_table[t_main_cluster_id].proxy_port + 1);
                        t_help_plan.set_k(k);
                        t_help_plan.set_g_m(g_m);
                        t_help_plan.set_l(l);
                        t_help_plan.set_block_size(block_size);
                        t_help_plan.set_encodetype(encode_type);
                        t_help_plan.set_is_local_repair(false);
                        for(int i = 0; i < int(t_main_plan.surviving_blockids_size()); i++)
                        {
                            t_help_plan.add_surviving_blockids(t_main_plan.surviving_blockids(i));
                        }
                        for(int i = 0; i < int(t_main_plan.failed_blockids_size()); i++)
                        {
                            t_help_plan.add_failed_blockids(t_main_plan.failed_blockids(i));
                        }
                        for(int i = 0; i < k + g_m + l; i++)
                        {
                            if(t_cluster_id == blocks_map[i]->map2cluster && !failed_map[i])
                            {
                                int t_node_id = blocks_map[i]->map2node;
                                t_help_plan.add_blockkeys(blocks_map[i]->block_key);
                                t_help_plan.add_blockids(blocks_map[i]->block_id);
                                t_help_plan.add_datanodeip(m_node_table[t_node_id].node_ip);
                                t_help_plan.add_datanodeport(m_node_table[t_node_id].node_port);

                                blocks_location->add_blockkeys(blocks_map[i]->block_key);
                                blocks_location->add_blockids(blocks_map[i]->block_id);
                                blocks_location->add_datanodeip(m_node_table[t_node_id].node_ip);
                                blocks_location->add_datanodeport(m_node_table[t_node_id].node_port);
                            }
                        }
                        if(t_cluster_id != t_main_cluster_id)
                            t_help_plans.push_back(t_help_plan);

                        blocks_location->set_proxy_ip(m_cluster_table[t_cluster_id].proxy_ip);
                        blocks_location->set_proxy_port(m_cluster_table[t_cluster_id].proxy_port);
                    }
                    t_main_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
                    t_main_plan.set_k(k);
                    t_main_plan.set_g_m(g_m);
                    t_main_plan.set_l(l);
                    t_main_plan.set_block_size(block_size);
                    t_main_plan.set_encodetype(encode_type);
                    t_main_plan.set_is_local_repair(false);
                    t_main_plan.set_s_cluster_id(t_main_cluster_id);
                    main_repair.push_back(t_main_plan);
                    help_repair.push_back(t_help_plans);
                
                    // update 
                    for(int i = 0; i < k + g_m + l; i++)
                    {
                        if(failed_map[i])
                        {
                            failed_map[i] = 0;
                            failed_blocks_num -= 1;
                            int group_id = blocks_map[i]->map2group;
                            fb_group_cnt[group_id] -= 1;
                        }
                    }
                }
                else
                {
                    std::cout << "Undecodable!!!" << std::endl;
                    return false;
                }
            }
            iter_cnt++;
        }
        return true;
    }     

    void CoordinatorImpl::simulation_repair(
        std::vector<proxy_proto::mainRepairPlan> &main_repair, int &cross_cluster_num)
    {
        for(int i = 0; i < int(main_repair.size()); i++)
        {
            int failed_block_num = int(main_repair[i].failed_blockkeys_size());
            int main_cluster_id = main_repair[i].s_cluster_id();
            for(int j = 0; j < int(main_repair[i].clusters_size()); j++)
            {
                int t_cluster_id = main_repair[i].clusters(j).cluster_id();
                if(main_cluster_id != t_cluster_id) // cross cluster
                {
                    int help_block_num = int(main_repair[i].clusters(j).blockkeys_size());
                    if(help_block_num > failed_block_num && m_encode_parameters.partial_decoding)
                    {
                        cross_cluster_num += failed_block_num;
                    }
                    else
                    {
                        cross_cluster_num += help_block_num;
                    }
                }
            }
        }
    }
}