#include "proxy.h"
#include "jerasure.h"
#include "reed_sol.h"
#include "tinyxml2.h"
#include "toolbox.h"
#include "lrc.h"
#include <thread>
#include <cassert>
#include <string>
#include <fstream>

inline std::string generate_string(int length) {
    static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    std::string random_string;
    random_string.reserve(length);

    for (int i = 0; i < length; i++) {
        random_string += alphanum[i % 62];
    }

    return random_string;
}

namespace ECProject
{
    grpc::Status ProxyImpl::mainRepair(
        grpc::ServerContext *context,
        const proxy_proto::mainRepairPlan *main_repair_plan,
        proxy_proto::RepairReply *response)
    {
        bool if_partial_decoding = main_repair_plan->if_partial_decoding();
        bool if_cross_cluster = if_partial_decoding;
        int k = main_repair_plan->k();
        int g_m = main_repair_plan->g_m();
        int l = main_repair_plan->l();
        int block_size = main_repair_plan->block_size();
        int encode_type = main_repair_plan->encodetype();
        bool is_local_repair = main_repair_plan->is_local_repair();

        // for failed blocks
        std::vector<std::string> failed_datanodeip;
        std::vector<int> failed_datanodeport;
        std::vector<std::string> failed_blockkeys;
        std::vector<int> failed_blockids;
        int failed_block_num = int(main_repair_plan->failed_blockkeys_size());
        int *erasures = new int[failed_block_num + 1];
        for(int i = 0; i < int(main_repair_plan->failed_blockkeys_size()); i++)
        {
            failed_blockkeys.push_back(main_repair_plan->failed_blockkeys(i));
            failed_blockids.push_back(main_repair_plan->failed_blockids(i));
            failed_datanodeip.push_back(main_repair_plan->failed_datanodeip(i));
            failed_datanodeport.push_back(main_repair_plan->failed_datanodeport(i));
            erasures[i] = main_repair_plan->failed_blockids(i);
        }
        erasures[failed_block_num] = -1;
        if(IF_DEBUG)
        {
            std::cout << "[Main Proxy " << m_self_cluster_id << "] To repair ";
            for(int i = 0; i < int(failed_blockkeys.size()); i++)
            {
                std::cout << failed_blockkeys[i] << " ";
            }
            std::cout << std::endl;
        }
        // for selected surviving blocks to repair
        auto svrs_idx_ptr = std::make_shared<std::vector<int>>();
        if(!is_local_repair)
        {
            for(int i = 0; i < int(main_repair_plan->surviving_blockids_size()); i++)
            {
                svrs_idx_ptr->push_back(main_repair_plan->surviving_blockids(i));
            }
        }
        // for help clusters
        std::vector<proxy_proto::locationInfo> help_locations;
        // for blocks in local datanodes
        std::vector<std::string> l_datanode_ip;
        std::vector<int> l_datanode_port;
        std::vector<std::string> l_blockkeys;
        std::vector<int> l_blockids;
        for(int i = 0; i < int(main_repair_plan->clusters_size()); i++)
        {
            if (int(main_repair_plan->clusters(i).cluster_id()) != m_self_cluster_id)
            {
                proxy_proto::locationInfo temp;
                temp.set_cluster_id(main_repair_plan->clusters(i).cluster_id());
                temp.set_proxy_ip(main_repair_plan->clusters(i).proxy_ip());
                temp.set_proxy_port(main_repair_plan->clusters(i).proxy_port());
                for(int j = 0; j < int(main_repair_plan->clusters(i).blockkeys_size()); j++)
                {
                    temp.add_blockkeys(main_repair_plan->clusters(i).blockkeys(j));
                    temp.add_blockids(main_repair_plan->clusters(i).blockids(j));
                    temp.add_datanodeip(main_repair_plan->clusters(i).datanodeip(j));
                    temp.add_datanodeport(main_repair_plan->clusters(i).datanodeport(j));
                }
                help_locations.push_back(temp);
            }
            else
            {
                for(int j = 0; j < main_repair_plan->clusters(i).blockkeys_size(); j++)
                {
                    l_blockids.push_back(main_repair_plan->clusters(i).blockids(j));
                    l_blockkeys.push_back(main_repair_plan->clusters(i).blockkeys(j));
                    l_datanode_ip.push_back(main_repair_plan->clusters(i).datanodeip(j));
                    l_datanode_port.push_back(main_repair_plan->clusters(i).datanodeport(j));
                }
            }
        }
        
        m_mutex.lock();
        m_if_repaired = false;
        m_mutex.unlock();

        try
        {
            auto lock_ptr = std::make_shared<std::mutex>();
            auto blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
            auto blocks_idx_ptr = std::make_shared<std::vector<int>>();
            auto getFromNode = [this, blocks_ptr, blocks_idx_ptr, lock_ptr](int block_idx, std::string block_key, int block_size, std::string node_ip, int node_port) mutable
            {
                std::vector<char> temp(block_size);
                bool ret = GetFromDatanode(block_key.c_str(), block_key.size(), temp.data(), block_size, node_ip.c_str(), node_port, block_idx + 2);
                if (!ret)
                {
                std::cout << "getFromNode !ret" << std::endl;
                return;
                }
                lock_ptr->lock();
                blocks_ptr->push_back(temp);
                blocks_idx_ptr->push_back(block_idx);
                lock_ptr->unlock();
            };

            auto h_lock_ptr = std::make_shared<std::mutex>();
            auto p_blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
            auto p_blocks_idx_ptr = std::make_shared<std::vector<int>>();
            auto getFromProxy = [this, h_lock_ptr, blocks_ptr, blocks_idx_ptr, p_blocks_ptr, p_blocks_idx_ptr, block_size, failed_block_num](std::shared_ptr<asio::ip::tcp::socket> socket_ptr) mutable
            {
                try
                {
                    asio::error_code ec;
                    std::vector<unsigned char> int_buf(sizeof(int));
                    asio::read(*socket_ptr, asio::buffer(int_buf, int_buf.size()), ec);
                    int t_cluster_id = ECProject::bytes_to_int(int_buf);
                    std::vector<unsigned char> int_flag_buf(sizeof(int));
                    asio::read(*socket_ptr, asio::buffer(int_flag_buf, int_flag_buf.size()), ec);
                    int t_flag = ECProject::bytes_to_int(int_flag_buf);
                    std::string msg = "data";
                    if(t_flag)
                        msg = "partial";
                    if (IF_DEBUG)
                    {
                        std::cout << "\033[1;36m" << "[Main Proxy " << m_self_cluster_id << "] Try to get " << msg << " blocks from the proxy in cluster " << t_cluster_id << ". " << t_flag << "\033[0m" << std::endl;
                    }
                    if(t_flag) // receive partial blocks from help proxies
                    {
                        h_lock_ptr->lock();
                        for (int j = 0; j < failed_block_num; j++)
                        {
                            std::vector<char> tmp_val(block_size);
                            asio::read(*socket_ptr, asio::buffer(tmp_val.data(), block_size), ec);
                            p_blocks_ptr->push_back(tmp_val);
                        }
                        p_blocks_idx_ptr->push_back(t_cluster_id);
                        h_lock_ptr->unlock();
                    }
                    else  // receive data blocks from help proxies
                    {
                        std::vector<unsigned char> int_buf_num_of_blocks(sizeof(int));
                        asio::read(*socket_ptr, asio::buffer(int_buf_num_of_blocks, int_buf_num_of_blocks.size()), ec);
                        int block_num = ECProject::bytes_to_int(int_buf_num_of_blocks);
                        for (int j = 0; j < block_num; j++)
                        {
                            std::vector<char> tmp_val(block_size);
                            std::vector<unsigned char> byte_block_id(sizeof(int));
                            asio::read(*socket_ptr, asio::buffer(byte_block_id, byte_block_id.size()), ec);
                            int block_idx = ECProject::bytes_to_int(byte_block_id);
                            asio::read(*socket_ptr, asio::buffer(tmp_val.data(), block_size), ec);
                            h_lock_ptr->lock();
                            blocks_ptr->push_back(tmp_val);
                            blocks_idx_ptr->push_back(block_idx);
                            h_lock_ptr->unlock();
                        }
                    }

                    if (IF_DEBUG)
                    {
                        std::cout << "\033[1;36m" << "[Main Proxy " << m_self_cluster_id << "] Finish getting data from the proxy in cluster " << t_cluster_id << "\033[0m" << std::endl;
                    }
                }
                catch (const std::exception &e)
                {
                std::cerr << e.what() << '\n';
                }
            };

            auto send_to_datanode = [this](int j, std::string block_key, char *data, int block_size, std::string s_node_ip, int s_node_port)
            {
                SetToDatanode(block_key.c_str(), block_key.size(), data, block_size, s_node_ip.c_str(), s_node_port, j + 2);
            };

            // get data blocks inside cluster
            int l_block_num = int(l_blockkeys.size());
            if(l_block_num > 0)
            {
                try
                {
                    std::vector<std::thread> read_threads;
                    for (int j = 0; j < l_block_num; j++)
                    {
                        read_threads.push_back(std::thread(getFromNode, l_blockids[j], l_blockkeys[j], block_size, l_datanode_ip[j], l_datanode_port[j]));
                    }
                    for (int j = 0; j < l_block_num; j++)
                    {
                        read_threads[j].join();
                    }
                }
                catch (const std::exception &e)
                {
                    std::cerr << e.what() << '\n';
                }
                if (l_block_num != int(blocks_ptr->size()))
                {
                    std::cout << "[Main] can't get enough blocks!" << std::endl;
                }
            }
            // get data blocks or partial blocks from other clusters
            int m_num = int(help_locations.size());
            int p_num = 0;
            if (IF_DEBUG)
            {
                std::cout << "[Main Proxy" << m_self_cluster_id << "] get data blocks from " << m_num << " helper proxy!" << std::endl;
            }
            try
            {
                std::vector<std::thread> read_p_threads;
                for (int j = 0; j < m_num; j++)
                {
                    int t_blocks_num = help_locations[j].blockkeys_size();
                    bool t_flag = true;
                    if(t_blocks_num <= failed_block_num)
                    {
                        t_flag = false;
                    }
                    t_flag = (if_partial_decoding && t_flag);
                    if (t_flag)
                    {
                        p_num += 1;
                        t_blocks_num = failed_block_num;
                        std::shared_ptr<asio::ip::tcp::socket> socket_ptr = std::make_shared<asio::ip::tcp::socket>(io_context);
                        acceptor.accept(*socket_ptr);
                        read_p_threads.push_back(std::thread(getFromProxy, socket_ptr));
                    }
                    else
                    {
                        l_block_num += t_blocks_num;
                        for(int jj = 0; jj < t_blocks_num; jj++)
                        {
                            read_p_threads.push_back(std::thread(getFromNode, help_locations[j].blockids(jj), help_locations[j].blockkeys(jj), 
                                                                 block_size, help_locations[j].datanodeip(jj), help_locations[j].datanodeport(jj)));
                        }
                    }

                    if (IF_DEBUG)
                    {
                        std::cout << "[Main Proxy" << m_self_cluster_id << "] cluster" << help_locations[j].cluster_id() << " blocks_num:" << help_locations[j].blockkeys_size() << std::endl;
                    }
                }
                int threads_num = int(read_p_threads.size());
                for (int j = 0; j < threads_num; j++)
                {
                    read_p_threads[j].join();
                }

                //simulate cross-cluster transfer
                if(if_cross_cluster)
                {
                    for (int j = 0; j < m_num; j++)
                    {
                        int t_blocks_num = help_locations[j].blockkeys_size();
                        bool t_flag = true;
                        if(t_blocks_num <= failed_block_num)
                        {
                            t_flag = false;
                        }
                        t_flag = (if_partial_decoding && t_flag);
                        if (t_flag)
                        {
                            t_blocks_num = failed_block_num;
                        }
                        std::string t_key = help_locations[j].blockkeys(0);
                        size_t t_value_length = block_size * t_blocks_num;
                        std::string temp_value = generate_string(t_value_length);
                        TransferToNetworkCore(t_key.c_str(), temp_value.c_str(), t_value_length, false);
                    }
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }

            struct timeval start_time, end_time;
            double t_time = 0.0;
            std::string et = "(U_LRC)";
            if(encode_type == NU_LRC)
            {
                et = "(NU_LRC)";
            }
            else if(encode_type == Azu_LRC)
            {
                et = "(Azu_LRC)";
            }
            int dp_block_num = int(blocks_idx_ptr->size());
            if(dp_block_num > 0 && (if_partial_decoding || is_local_repair))
            {
                std::vector<char *> v_data(dp_block_num);
                std::vector<char *> v_coding(failed_block_num);
                char **data = (char **)v_data.data();
                char **coding = (char **)v_coding.data();
                std::vector<std::vector<char>> v_data_area(dp_block_num, std::vector<char>(block_size));
                for (int j = 0; j < dp_block_num; j++)
                {
                    data[j] = v_data_area[j].data();
                }
                for (int j = 0; j < dp_block_num; j++)
                {
                    memcpy(data[j], (*blocks_ptr)[j].data(), block_size);
                }
                std::vector<std::vector<char>> v_coding_area(failed_block_num, std::vector<char>(block_size));
                for (int j = 0; j < failed_block_num; j++)
                {
                    coding[j] = v_coding_area[j].data();
                }
                gettimeofday(&start_time, NULL);
                if(!is_local_repair)
                {
                    if(encode_type == NU_LRC)
                    {
                        auto cp_ptr = std::make_shared<std::vector<fp>>();
                        int n_1 = main_repair_plan->ki_size();
                        for(int i = 0; i < n_1; i++)
                        {
                            fp tmp;
                            tmp.ki = (ushort)main_repair_plan->ki(i);
                            tmp.ri = (ushort)main_repair_plan->ri(i);
                            cp_ptr->push_back(tmp);
                        }
                        encode_partial_blocks_for_decoding_Non_Uniform_LRC(k, g_m, l, data, coding, block_size, blocks_idx_ptr, svrs_idx_ptr, std::make_shared<std::vector<int>>(failed_blockids), cp_ptr);
                    }
                    else if(encode_type == U_LRC)
                    {
                        encode_partial_blocks_for_decoding_Uniform_LRC(k, g_m, l, data, coding, block_size, blocks_idx_ptr, svrs_idx_ptr, std::make_shared<std::vector<int>>(failed_blockids));
                    }
                    else if(encode_type == Azu_LRC)
                    {
                        encode_partial_blocks_for_decoding_Azu_LRC(k, g_m, l, data, coding, block_size, blocks_idx_ptr, svrs_idx_ptr, std::make_shared<std::vector<int>>(failed_blockids));
                    }
                }
                else
                {
                    perform_addition(data, coding, block_size, dp_block_num, failed_block_num);
                }
                gettimeofday(&end_time, NULL);
                t_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

                h_lock_ptr->lock();
                for (int j = 0; j < failed_block_num; j++)
                {
                    p_blocks_ptr->push_back(v_coding_area[j]);
                }
                p_blocks_idx_ptr->push_back(m_self_cluster_id);
                h_lock_ptr->unlock();

                p_num += 1;
            }

            if (IF_DEBUG)
            {
                std::cout << "[Main Proxy" << m_self_cluster_id << "] ready to decode! " << dp_block_num << " " << p_num << std::endl;
            }

            // decode
            int count = l_block_num;
            if (if_partial_decoding || is_local_repair)
            {
                count = p_num * failed_block_num;
            }
            if(!if_cross_cluster)
            {
                std::string t_key = "temp";
                size_t t_value_length = block_size * l_block_num;
                std::string temp_value = generate_string(t_value_length);
                TransferToNetworkCore(t_key.c_str(), temp_value.c_str(), t_value_length, false);
            }
            int coding_num = failed_block_num;
            int data_num = count;
            if(!(if_partial_decoding || is_local_repair))
            {
                data_num = k;
                coding_num = g_m + l;
            }
            std::vector<char *> vt_data(data_num);
            std::vector<char *> vt_coding(coding_num);
            char **t_data = (char **)vt_data.data();
            char **t_coding = (char **)vt_coding.data();
            std::vector<std::vector<char>> vt_data_area(data_num, std::vector<char>(block_size));
            std::vector<std::vector<char>> vt_coding_area(coding_num, std::vector<char>(block_size));
            if (IF_DEBUG)
            {
                std::cout << "[Main Proxy" << m_self_cluster_id << "] " << count << " " << p_blocks_ptr->size() << " " << blocks_ptr->size() << std::endl;
            }
            for (int j = 0; j < data_num; j++)
            {
                t_data[j] = vt_data_area[j].data();
            }
            for (int j = 0; j < coding_num; j++)
            {
                t_coding[j] = vt_coding_area[j].data();
            }
            if (if_partial_decoding || is_local_repair)  //decode with partial blocks
            {
                for(int j = 0; j < count; j++)
                {
                    memcpy(t_data[j], (*p_blocks_ptr)[j].data(), block_size);
                }
            }
            else  //decode with data blocks
            {
                for(int j = 0; j < count; j++)
                {
                    int idx = (*blocks_idx_ptr)[j];
                    if(idx >= k)
                    {
                        memcpy(t_coding[idx - k], (*blocks_ptr)[j].data(), block_size);
                    }
                    else
                    {
                        memcpy(t_data[idx], (*blocks_ptr)[j].data(), block_size);
                    }
                }
            }
            if (IF_DEBUG)
            {
                std::cout << "[Main Proxy" << m_self_cluster_id << "] encoding!" << std::endl;
            }
            try
            {
                gettimeofday(&start_time, NULL);
                if(if_partial_decoding || is_local_repair)
                {
                    if(data_num == failed_block_num && data_num == 1)
                    {
                        t_coding = t_data;
                    }
                    else
                    {
                        perform_addition(t_data, t_coding, block_size, data_num, failed_block_num);
                    }
                }
                else
                {
                    if(encode_type == U_LRC)
                    {
                        decode_Uniform_LRC(k, g_m, l, t_data, t_coding, block_size, erasures, failed_block_num);
                    }
                    else if(encode_type == NU_LRC)
                    {
                        auto cp_ptr = std::make_shared<std::vector<fp>>();
                        int n_1 = main_repair_plan->ki_size();
                        for(int i = 0; i < n_1; i++)
                        {
                            fp tmp;
                            tmp.ki = (ushort)main_repair_plan->ki(i);
                            tmp.ri = (ushort)main_repair_plan->ri(i);
                            cp_ptr->push_back(tmp);
                        }
                        decode_Non_Uniform_LRC(k, g_m, l, t_data, t_coding, block_size, erasures, failed_block_num, cp_ptr);
                    }
                    else if(encode_type == Azu_LRC)
                    {
                        decode_Azu_LRC(k, g_m, l, t_data, t_coding, block_size, erasures, failed_block_num);
                    }
                }
                gettimeofday(&end_time, NULL);
                t_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
                std::cout << et << "Main Decode Time: " << t_time << " (block_size=" << block_size / (1024 * 1024) << "MB)." << std::endl;
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }

            try
            {
                std::vector<std::thread> set_threads;
                for(int i = 0; i < failed_block_num; i++)
                {
                    if (IF_DEBUG)
                    {
                        std::cout << "[Main Proxy" << m_self_cluster_id << "] set " << failed_blockkeys[i] << " to datanode " << failed_datanodeport[i] << std::endl;
                    }
                    if(!if_partial_decoding)
                    {
                        int index = failed_blockids[i];
                        if(index >= k)
                        {
                            set_threads.push_back(std::thread(send_to_datanode, i, failed_blockkeys[i], t_coding[index - k], block_size, failed_datanodeip[i], failed_datanodeport[i]));
                        }
                        else
                        {
                            set_threads.push_back(std::thread(send_to_datanode, i, failed_blockkeys[i], t_data[index], block_size, failed_datanodeip[i], failed_datanodeport[i]));
                        }
                    }
                    else
                    {
                        set_threads.push_back(std::thread(send_to_datanode, i, failed_blockkeys[i], t_coding[i], block_size, failed_datanodeip[i], failed_datanodeport[i]));
                    }
                }
                for (int i = 0; i < failed_block_num; i++)
                {
                    set_threads[i].join();
                }

                if(if_cross_cluster)
                {
                    //simulate cross-cluster transfer
                    for (int j = 0; j < failed_block_num; j++)
                    {
                        std::string node_ip_port = failed_datanodeip[j] + ":" + std::to_string(failed_datanodeport[j]);
                        int t_cluster_id = m_datanode2cluster[node_ip_port];
                        if(t_cluster_id != m_self_cluster_id)
                        {
                            size_t t_value_length = block_size;
                            std::string t_key = failed_blockkeys[j];
                            std::string temp_value = generate_string(t_value_length);
                            TransferToNetworkCore(t_key.c_str(), temp_value.c_str(), t_value_length, false);
                        }
                    }
                }
            }
            catch(const std::exception& e)
            {
                std::cerr << e.what() << '\n';
            }
            // for check
            m_if_repaired = true;
            cv.notify_all();
            if(IF_DEBUG)
            {
                std::cout << "\033[1;32m[Main Proxy" << m_self_cluster_id << "] finish repair " << failed_block_num << " blocks! " << "\033[0m" << std::endl;
            }
        }
        catch(const std::exception& e)
        {
        std::cerr << e.what() << '\n';
        }
        return grpc::Status::OK;
    }
    grpc::Status ProxyImpl::helpRepair(
        grpc::ServerContext *context,
        const proxy_proto::helpRepairPlan *help_repair_plan,
        proxy_proto::RepairReply *response)
    {
        bool if_partial_decoding = help_repair_plan->if_partial_decoding();
        std::string proxy_ip = help_repair_plan->mainproxyip();
        int proxy_port = help_repair_plan->mainproxyport();
        int k = help_repair_plan->k();
        int g_m = help_repair_plan->g_m();
        int l = help_repair_plan->l();
        int block_size = help_repair_plan->block_size();
        bool is_local_repair = help_repair_plan->is_local_repair();
        ECProject::EncodeType encode_type = (ECProject::EncodeType)help_repair_plan->encodetype();
        
        std::vector<std::string> datanode_ip;
        std::vector<int> datanode_port;
        std::vector<std::string> blockkeys;
        std::vector<int> blockids;
        for (int i = 0; i < help_repair_plan->blockkeys_size(); i++)
        {
            datanode_ip.push_back(help_repair_plan->datanodeip(i));
            datanode_port.push_back(help_repair_plan->datanodeport(i));
            blockkeys.push_back(help_repair_plan->blockkeys(i));
            blockids.push_back(help_repair_plan->blockids(i));
        }

        // for failed blocks
        auto fls_idx_ptr = std::make_shared<std::vector<int>>();
        int failed_block_num = int(help_repair_plan->failed_blockids_size());
        for(int i = 0; i < failed_block_num; i++)
        {
            fls_idx_ptr->push_back(help_repair_plan->failed_blockids(i));
        }
        // for selected surviving blocks to repair
        auto svrs_idx_ptr = std::make_shared<std::vector<int>>();
        if(!is_local_repair)
        {
            int svrs_num = int(help_repair_plan->surviving_blockids_size());
            for(int i = 0; i < svrs_num; i++)
            {
                svrs_idx_ptr->push_back(help_repair_plan->surviving_blockids(i));
            }
        }

        bool flag = true;
        if(int(blockkeys.size()) <= failed_block_num)
        {
        flag = false;
        }
        if_partial_decoding = (if_partial_decoding && flag);

        // get data from the datanode
        auto myLock_ptr = std::make_shared<std::mutex>();
        auto blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
        auto blocks_idx_ptr = std::make_shared<std::vector<int>>();
        auto getFromNode = [this, block_size, blocks_ptr, blocks_idx_ptr, myLock_ptr](int block_idx, std::string block_key, std::string node_ip, int node_port) mutable
        {
            std::vector<char> temp(block_size);
            bool ret = GetFromDatanode(block_key.c_str(), block_key.size(), temp.data(), block_size, node_ip.c_str(), node_port, block_idx + 2);

            if (!ret)
            {
                std::cout << "getFromNode !ret" << std::endl;
                return;
            }
            myLock_ptr->lock();
            blocks_ptr->push_back(temp);
            blocks_idx_ptr->push_back(block_idx);
            myLock_ptr->unlock();
        };
        if (IF_DEBUG)
        {
            std::cout << "[Helper Proxy" << m_self_cluster_id << "] Ready to read blocks from data node!" << std::endl;
        }

        int block_num = int(blockkeys.size());
        try
        {
            std::vector<std::thread> read_threads;
            for (int j = 0; j < block_num; j++)
            {
                read_threads.push_back(std::thread(getFromNode, blockids[j], blockkeys[j], datanode_ip[j], datanode_port[j]));
            }
            for (int j = 0; j < block_num; j++)
            {
                read_threads[j].join();
            }
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << '\n';
        }
    
        if (block_num != int(blocks_ptr->size()))
        {
            std::cout << "[Help] can't get enough blocks!" << std::endl;
        }

        std::vector<char *> v_data(block_num);
        std::vector<char *> v_coding(failed_block_num);
        char **data = (char **)v_data.data();
        char **coding = (char **)v_coding.data();
        std::vector<std::vector<char>> v_data_area(block_num, std::vector<char>(block_size));
        std::vector<std::vector<char>> v_coding_area(failed_block_num, std::vector<char>(block_size));
        for (int j = 0; j < block_num; j++)
        {
            data[j] = v_data_area[j].data();
        }
        for (int j = 0; j < failed_block_num; j++)
        {
            coding[j] = v_coding_area[j].data();
        }
        for (int j = 0; j < block_num; j++)
        {
            memcpy(data[j], (*blocks_ptr)[j].data(), block_size);
        }

        if (IF_DEBUG)
        {
            std::cout << "[Helper Proxy" << m_self_cluster_id << "] partial encoding!" << std::endl;
            for(auto it = blocks_idx_ptr->begin(); it != blocks_idx_ptr->end(); it++)
            {
                std::cout << (*it) << " ";
            }
            std::cout << std::endl;
        }

        if(if_partial_decoding)
        {
            struct timeval start_time, end_time;
            double t_time = 0.0;
            std::string et = "(U_LRC)";
            if(encode_type == NU_LRC)
            {
                et = "(NU_LRC)";
            }
            else if(encode_type == Azu_LRC)
            {
                et = "(Azu_LRC)";
            }
            gettimeofday(&start_time, NULL);
            if(!is_local_repair)
            {
                if(encode_type == NU_LRC)
                {
                    auto cp_ptr = std::make_shared<std::vector<fp>>();
                    int n_1 = help_repair_plan->ki_size();
                    for(int i = 0; i < n_1; i++)
                    {
                        fp tmp;
                        tmp.ki = (ushort)help_repair_plan->ki(i);
                        tmp.ri = (ushort)help_repair_plan->ri(i);
                        cp_ptr->push_back(tmp);
                    }
                    encode_partial_blocks_for_decoding_Non_Uniform_LRC(k, g_m, l, data, coding, block_size, blocks_idx_ptr, svrs_idx_ptr, fls_idx_ptr, cp_ptr);
                }
                else if(encode_type == U_LRC)
                {
                    encode_partial_blocks_for_decoding_Uniform_LRC(k, g_m, l, data, coding, block_size, blocks_idx_ptr, svrs_idx_ptr, fls_idx_ptr);
                }
                else if(encode_type == Azu_LRC)
                {
                    encode_partial_blocks_for_decoding_Azu_LRC(k, g_m, l, data, coding, block_size, blocks_idx_ptr, svrs_idx_ptr, fls_idx_ptr);
                }
            }
            else
            {
                perform_addition(data, coding, block_size, block_num, failed_block_num);
            }
            gettimeofday(&end_time, NULL);
            t_time = end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
            std::cout << et << "Help Decode Time: " << t_time << " (block_size=" << block_size / (1024 * 1024) << "MB)." << std::endl;
        }

        // send to main proxy
        asio::error_code error;
        asio::io_context io_context;
        asio::ip::tcp::socket socket(io_context);
        asio::ip::tcp::resolver resolver(io_context);
        asio::error_code con_error;
        if (IF_DEBUG)
        {
        std::cout << "\033[1;36m[Helper Proxy" << m_self_cluster_id << "] Try to connect main proxy port " << proxy_port << "\033[0m" << std::endl;
        }
        asio::connect(socket, resolver.resolve({proxy_ip, std::to_string(proxy_port)}), con_error);
        if (!con_error && IF_DEBUG)
        {
        std::cout << "\033[1;36m[Helper Proxy" << m_self_cluster_id << "] Connect to " << proxy_ip << ":" << proxy_port << " success!" << std::endl;
        }

        int value_size = 0;
    
        std::vector<unsigned char> int_buf_self_cluster_id = ECProject::int_to_bytes(m_self_cluster_id);
        asio::write(socket, asio::buffer(int_buf_self_cluster_id, int_buf_self_cluster_id.size()), error);
        if (!if_partial_decoding)
        {
            std::vector<unsigned char> t_flag = ECProject::int_to_bytes(0);
            asio::write(socket, asio::buffer(t_flag, t_flag.size()), error);
            std::vector<unsigned char> int_buf_num_of_blocks = ECProject::int_to_bytes(block_num);
            asio::write(socket, asio::buffer(int_buf_num_of_blocks, int_buf_num_of_blocks.size()), error);
            int j = 0;
            for(auto it = blocks_idx_ptr->begin(); it != blocks_idx_ptr->end(); it++, j++)
            { 
                // send index and value
                int block_idx = *it;
                std::vector<unsigned char> byte_block_idx = ECProject::int_to_bytes(block_idx);
                asio::write(socket, asio::buffer(byte_block_idx, byte_block_idx.size()), error);
                asio::write(socket, asio::buffer(data[j], block_size), error);
                value_size += block_size;
            }
        }
        else
        {
            std::vector<unsigned char> t_flag = ECProject::int_to_bytes(1);
            asio::write(socket, asio::buffer(t_flag, t_flag.size()), error);
            for (int j = 0; j < failed_block_num; j++)
            {
                asio::write(socket, asio::buffer(coding[j], block_size), error);
                value_size += block_size;
            }
        }
        if (IF_DEBUG)
        {
        std::cout << "[Helper Proxy" << m_self_cluster_id << "] Send value to proxy" << proxy_port << "! With length of " << value_size << std::endl;
        }
        return grpc::Status::OK;
    }
} // namespace ECProject