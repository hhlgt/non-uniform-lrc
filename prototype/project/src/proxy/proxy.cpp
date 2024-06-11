#include "jerasure.h"
#include "reed_sol.h"
#include "tinyxml2.h"
#include "toolbox.h"
#include "proxy.h"
#include "lrc.h"
#include <thread>
#include <cassert>
#include <string>
#include <fstream>

template <typename T>
inline T ceil(T const &A, T const &B)
{
  return T((A + B - 1) / B);
};

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
  bool ProxyImpl::init_coordinator()
  {
    m_coordinator_ptr = coordinator_proto::coordinatorService::NewStub(grpc::CreateChannel(m_coordinator_address, grpc::InsecureChannelCredentials()));
    // coordinator_proto::RequestToCoordinator req;
    // coordinator_proto::ReplyFromCoordinator rep;
    // grpc::ClientContext context;
    // std::string proxy_info = "Proxy [" + proxy_ip_port + "]";
    // req.set_name(proxy_info);
    // grpc::Status status;
    // status = m_coordinator_ptr->checkalive(&context, req, &rep);
    // if (status.ok())
    // {
    //   // std::cout << "[Coordinator Check] ok from " << m_coordinator_address << std::endl;
    // }
    // else
    // {
    //   std::cout << "[Coordinator Check] failed to connect " << m_coordinator_address << std::endl;
    // }
    return true;
  }

  bool ProxyImpl::init_datanodes(std::string m_datanodeinfo_path)
  {
    tinyxml2::XMLDocument xml;
    xml.LoadFile(m_datanodeinfo_path.c_str());
    tinyxml2::XMLElement *root = xml.RootElement();
    for (tinyxml2::XMLElement *cluster = root->FirstChildElement(); cluster != nullptr; cluster = cluster->NextSiblingElement())
    {
      std::string cluster_id(cluster->Attribute("id"));
      std::string proxy(cluster->Attribute("proxy"));
      if (proxy == proxy_ip_port)
      {
        m_self_cluster_id = std::stoi(cluster_id);
      }
      for (tinyxml2::XMLElement *node = cluster->FirstChildElement()->FirstChildElement(); node != nullptr; node = node->NextSiblingElement())
      {
        std::string node_uri(node->Attribute("uri"));
        auto _stub = datanode_proto::datanodeService::NewStub(grpc::CreateChannel(node_uri, grpc::InsecureChannelCredentials()));
        // datanode_proto::CheckaliveCMD cmd;
        // datanode_proto::RequestResult result;
        // grpc::ClientContext context;
        // std::string proxy_info = "Proxy [" + proxy_ip_port + "]";
        // cmd.set_name(proxy_info);
        // grpc::Status status;
        // status = _stub->checkalive(&context, cmd, &result);
        // if (status.ok())
        // {
        //   std::cout << "[Datanode Check] ok from " << node_uri << std::endl;
        // }
        // else
        // {
        //   std::cout << "[Datanode Check] failed to connect " << node_uri << std::endl;
        // }
        m_datanode_ptrs.insert(std::make_pair(node_uri, std::move(_stub)));
        m_datanode2cluster.insert(std::make_pair(node_uri, std::stoi(cluster_id)));
      }
    }
    // init networkcore
    auto _stub = datanode_proto::datanodeService::NewStub(grpc::CreateChannel(m_networkcore, grpc::InsecureChannelCredentials()));
    m_datanode_ptrs.insert(std::make_pair(m_networkcore, std::move(_stub)));
    return true;
  }

  grpc::Status ProxyImpl::checkalive(grpc::ServerContext *context,
                                     const proxy_proto::CheckaliveCMD *request,
                                     proxy_proto::RequestResult *response)
  {

    std::cout << "[Proxy] checkalive" << request->name() << std::endl;
    response->set_message(false);
    init_coordinator();
    return grpc::Status::OK;
  }

  bool ProxyImpl::SetToDatanode(const char *key, size_t key_length, const char *value, size_t value_length, const char *ip, int port, int offset)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::SetInfo set_info;
      datanode_proto::RequestResult result;
      set_info.set_block_key(std::string(key));
      set_info.set_block_size(value_length);
      set_info.set_proxy_ip(m_ip);
      set_info.set_proxy_port(m_port + offset);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleSet(&context, set_info, &result);

      asio::error_code error;
      asio::io_context io_context;
      asio::ip::tcp::socket socket(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      asio::error_code con_error;
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + 20)}), con_error);
      if (!con_error && IF_DEBUG)
      {
        std::cout << "Connect to " << ip << ":" << port + 20 << " success!" << std::endl;
      }

      asio::write(socket, asio::buffer(value, value_length), error);

      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                  << "Write " << key << " to socket finish! With length of " << strlen(value) << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::GetFromDatanode(const char *key, size_t key_length, char *value, size_t value_length, const char *ip, int port, int offset)
  {
    try
    {
      // ready to recieve
      char *buf = new char[value_length];
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "Ready to recieve data from datanode " << std::endl;
      }

      grpc::ClientContext context;
      datanode_proto::GetInfo get_info;
      datanode_proto::RequestResult result;
      get_info.set_block_key(std::string(key));
      get_info.set_block_size(value_length);
      get_info.set_proxy_ip(m_ip);
      get_info.set_proxy_port(m_port + offset);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleGet(&context, get_info, &result);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "Call datanode to handle get " << key << std::endl;
      }

      asio::io_context io_context;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::socket socket(io_context);
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + 20)}));
      asio::error_code ec;
      asio::read(socket, asio::buffer(buf, value_length), ec);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "Read data from socket with length of " << value_length << std::endl;
      }
      memcpy(value, buf, value_length);
      delete buf;
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::DelInDatanode(std::string key, std::string node_ip_port)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::DelInfo delinfo;
      datanode_proto::RequestResult response;
      delinfo.set_block_key(key);
      grpc::Status status = m_datanode_ptrs[node_ip_port]->handleDelete(&context, delinfo, &response);
      if (status.ok() && IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][DEL] delete block " << key << " success!" << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::TransferToNetworkCore(const char *key, const char *value, size_t value_length, bool ifset)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::TransferInfo trans_info;
      datanode_proto::RequestResult result;
      trans_info.set_value_key(std::string(key));
      trans_info.set_value_size(value_length);
      trans_info.set_ifset(ifset);
      grpc::Status stat = m_datanode_ptrs[m_networkcore]->handleTransfer(&context, trans_info, &result);

      std::string ip;
      int port;
      std::stringstream ss(m_networkcore);
      std::getline(ss, ip, ':');
      ss >> port;

      if(IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Cross-Cluster Transfer] " << m_networkcore << " address " << ip << ":" << port << std::endl;
      }

      asio::error_code error;
      asio::io_context io_context;
      asio::ip::tcp::socket socket(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      asio::error_code con_error;
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + 20)}), con_error);
      if (!con_error && IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Cross-Cluster Transfer]" << "Connect to " << ip << ":" << port + 20 << " success!" << std::endl;
      }

      asio::write(socket, asio::buffer(value, value_length), error);

      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Cross-Cluster Transfer]"
                  << "Write " << key << " to socket finish! With length of " << strlen(value) << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  grpc::Status ProxyImpl::encodeAndSetObject(
      grpc::ServerContext *context,
      const proxy_proto::ObjectAndPlacement *object_and_placement,
      proxy_proto::SetReply *response)
  {
    std::string key = object_and_placement->key();
    int value_size_bytes = object_and_placement->valuesizebyte();
    int k = object_and_placement->k();
    int g_m = object_and_placement->g_m();
    int l = object_and_placement->l();
    int stripe_id = object_and_placement->stripe_id();
    int block_size = object_and_placement->block_size();
    ECProject::EncodeType encode_type = (ECProject::EncodeType)object_and_placement->encode_type();
    std::vector<std::pair<std::string, std::pair<std::string, int>>> keys_nodes;
    for (int i = 0; i < object_and_placement->datanodeip_size(); i++)
    {
      keys_nodes.push_back(std::make_pair(object_and_placement->blockkeys(i), std::make_pair(object_and_placement->datanodeip(i), object_and_placement->datanodeport(i))));
    }
    auto cp_ptr = std::make_shared<std::vector<fp>>();
    if(encode_type == NU_LRC)
    {
      int n_1 = object_and_placement->ki_size();
      for(int i = 0; i < n_1; i++)
      {
        fp tmp;
        tmp.ki = (ushort)object_and_placement->ki(i);
        tmp.ri = (ushort)object_and_placement->ri(i);
        cp_ptr->push_back(tmp);
      }
    }
    auto encode_and_save = [this, key, value_size_bytes, k, g_m, l, block_size, keys_nodes, encode_type, cp_ptr, stripe_id]() mutable
    {
      try
      {
        // read the key and value in the socket sent by client
        asio::ip::tcp::socket socket_data(io_context);
        acceptor.accept(socket_data);
        asio::error_code error;

        int extend_value_size_byte = block_size * k;
        if(value_size_bytes > extend_value_size_byte)
        {
          std::cout << "length not matches!" << std::endl;
        }
        std::vector<char> buf_key(key.size());
        std::vector<char> v_buf(extend_value_size_byte);
        for (int i = value_size_bytes; i < extend_value_size_byte; i++)
        {
          v_buf[i] = '0';
        }

        asio::read(socket_data, asio::buffer(buf_key, key.size()), error);
        if (error == asio::error::eof)
        {
          std::cout << "error == asio::error::eof" << std::endl;
        }
        else if (error)
        {
          throw asio::system_error(error);
        }
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "Check key " << buf_key.data() << std::endl;
        }
        // check the key
        bool flag = true;
        for (int i = 0; i < int(key.size()); i++)
        {
          if (key[i] != buf_key[i])
          {
            flag = false;
          }
        }
        if (flag)
        {
          if (IF_DEBUG)
          {
            std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                      << "Read value of " << buf_key.data() << " with length of " 
                      << value_size_bytes << ", extend = " << extend_value_size_byte << std::endl;
          }
          asio::read(socket_data, asio::buffer(v_buf.data(), value_size_bytes), error);
        }
        asio::error_code ignore_ec;
        socket_data.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
        socket_data.close(ignore_ec);

        // set the blocks to the datanode
        char *buf = v_buf.data();
        auto send_to_datanode = [this](int j, int k, std::string block_key, char **data, char **coding, int block_size, std::pair<std::string, int> ip_and_port)
        {
          if (IF_DEBUG)
          {
            std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                      << "Thread " << j << " send " << block_key << " to Datanode" << ip_and_port.second << std::endl;
          }
          if (j < k)
          {
            SetToDatanode(block_key.c_str(), block_key.size(), data[j], block_size, ip_and_port.first.c_str(), ip_and_port.second, j + 2);
          }
          else
          {
            SetToDatanode(block_key.c_str(), block_key.size(), coding[j - k], block_size, ip_and_port.first.c_str(), ip_and_port.second, j + 2);
          }
        };
        // calculate parity blocks
        std::vector<char *> v_data(k);
        std::vector<char *> v_coding(g_m + l);
        char **data = (char **)v_data.data();
        char **coding = (char **)v_coding.data();

        std::vector<std::vector<char>> v_coding_area(g_m + l, std::vector<char>(block_size));
        for (int j = 0; j < k; j++)
        {
          data[j] = &buf[j * block_size];
        }
        for (int j = 0; j < g_m + l; j++)
        {
          coding[j] = v_coding_area[j].data();
        }
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "Encode value with size of " << v_buf.size() << ", k-g-l = " << k << "-" << g_m << "-" << l << std::endl;
        }
        int send_num = k + g_m + l;
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
        if(encode_type == U_LRC)
        {
          encode_Uniform_LRC(k, g_m, l, data, coding, block_size);
        }
        else if(encode_type == Azu_LRC)
        {
          encode_Azu_LRC(k, g_m, l, data, coding, block_size);
        }
        else
        {
          encode_Non_Uniform_LRC(k, g_m, l, data, coding, block_size, cp_ptr);
        }
        gettimeofday(&end_time, NULL);
        t_time = end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
        std::cout << et << "Encode Time: " << t_time << " (block_size=" << block_size / (1024 * 1024) << "MB)." << std::endl;
        
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "Distribute blocks to datanodes" << std::endl;
        }
        std::vector<std::thread> senders;
        for (int j = 0; j < send_num; j++)
        {
          std::string block_key = keys_nodes[j].first;
          std::pair<std::string, int> &ip_and_port = keys_nodes[j].second;
          senders.push_back(std::thread(send_to_datanode, j, k, block_key, data, coding, block_size, ip_and_port));
        }
        for (int j = 0; j < int(senders.size()); j++)
        {
          senders[j].join();
        }

        if(IF_TEST_THROUGHPUT)
        {
          // simulate cross-cluster transfer
          int cross_cluster_num = 0;
          for(int j = 0; j < send_num; j++)
          {
            std::pair<std::string, int> &ip_and_port = keys_nodes[j].second;
            std::string ip_port = ip_and_port.first + ":" + std::to_string(ip_and_port.second);
            int t_cluster_id = m_datanode2cluster[ip_port];
            if(t_cluster_id != m_self_cluster_id)
            {
              cross_cluster_num++;
            }
          }
          if(cross_cluster_num > 0)
          {
            std::string temp_key = "temp";
            int val_len = block_size * cross_cluster_num;
            std::string temp_value = generate_string(val_len);
            TransferToNetworkCore(temp_key.c_str(), temp_value.c_str(), val_len, false);
          }
        }

        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "Finish distributing blocks!" << std::endl;
        }
        coordinator_proto::CommitAbortStripe commit_abort_stripe;
        coordinator_proto::ReplyFromCoordinator result;
        grpc::ClientContext context;
        ECProject::OpperateType opp = SET;
        commit_abort_stripe.set_opp(opp);
        commit_abort_stripe.set_stripe_id(stripe_id);
        commit_abort_stripe.set_ifcommitmetadata(true);
        grpc::Status status;
        status = m_coordinator_ptr->reportCommitAbort(&context, commit_abort_stripe, &result);
        if (status.ok() && IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << " report to coordinator success" << std::endl;
        }
        else
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << " report to coordinator fail!" << std::endl;
        }
      }
      catch (std::exception &e)
      {
        std::cout << "exception in encode_and_save" << std::endl;
        std::cout << e.what() << std::endl;
      }
    };
    try
    {
      if (IF_DEBUG)
      {
        std::cout << "[Proxy][SET] Handle encode and set" << std::endl;
      }
      std::thread my_thread(encode_and_save);
      my_thread.detach();
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::decodeAndGetObject(
      grpc::ServerContext *context,
      const proxy_proto::GetObject *object_info,
      proxy_proto::GetReply *response)
  {
    std::string object_key = object_info->object_key();
    int block_size = object_info->block_size();
    int block_num = object_info->block_num();
    int start_id = object_info->start_id();
    std::string clientip = object_info->clientip();
    int clientport = object_info->clientport();
    int value_size_bytes = block_size * block_num;

    std::vector<std::pair<std::string, std::pair<std::string, int>>> keys_nodes;
    std::vector<int> block_idxs;
    for (int i = 0; i < object_info->datanodeip_size(); i++)
    {
      block_idxs.push_back(object_info->blockids(i));
      keys_nodes.push_back(std::make_pair(object_info->blockkeys(i), std::make_pair(object_info->datanodeip(i), object_info->datanodeport(i))));
    }

    auto decode_and_get = [this, object_key, start_id, block_num, block_size, value_size_bytes, 
                           clientip, clientport, keys_nodes, block_idxs]() mutable
    {
      auto blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
      auto blocks_idx_ptr = std::make_shared<std::vector<int>>();
      auto myLock_ptr = std::make_shared<std::mutex>();
      auto cv_ptr = std::make_shared<std::condition_variable>();

      std::vector<char *> v_data(block_num);
      char **data = v_data.data();

      auto getFromNode = [this, start_id, block_num, blocks_ptr, blocks_idx_ptr, myLock_ptr, cv_ptr](int block_idx, std::string block_key, int block_size, std::string ip, int port)
      {
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                    << "Block " << block_idx << " with key " << block_key << " from Datanode" << ip << ":" << port << std::endl;
        }

        std::vector<char> temp(block_size);
        bool ret = GetFromDatanode(block_key.c_str(), block_key.size(), temp.data(), block_size, ip.c_str(), port, block_idx + 2);

        if (!ret)
        {
          std::cout << "getFromNode !ret" << std::endl;
          return;
        }
        // directly read corresponding data blocks
        myLock_ptr->lock();
        blocks_ptr->push_back(temp);
        blocks_idx_ptr->push_back(block_idx);
        myLock_ptr->unlock();
      };

      std::vector<std::vector<char>> v_data_area(block_num, std::vector<char>(block_size));
      for (int j = 0; j < block_num; j++)
      {
        data[j] = v_data_area[j].data();
      }
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "ready to get blocks from datanodes!" << std::endl;
      }
      std::vector<std::thread> read_treads;
      for (int j = 0; j < block_num; j++)
      {
        int block_idx = block_idxs[j];
        std::string block_key = keys_nodes[j].first;
        std::pair<std::string, int> &ip_and_port = keys_nodes[j].second;
        read_treads.push_back(std::thread(getFromNode, block_idx, block_key, block_size, ip_and_port.first, ip_and_port.second));
      }
      for (int j = 0; j < block_num; j++)
      {
        read_treads[j].join();
      }

      if(IF_TEST_THROUGHPUT)
      {
        // simulate cross-cluster transfer
        int cross_cluster_num = 0;
        for(int j = 0; j < block_num; j++)
        {
          std::pair<std::string, int> &ip_and_port = keys_nodes[j].second;
          std::string ip_port = ip_and_port.first + ":" + std::to_string(ip_and_port.second);
          int t_cluster_id = m_datanode2cluster[ip_port];
          if(t_cluster_id != m_self_cluster_id)
          {
            cross_cluster_num++;
          }
        }
        if(cross_cluster_num > 0)
        {
          std::string temp_key = "temp";
          int val_len = block_size * cross_cluster_num;
          std::string temp_value = generate_string(val_len);
          TransferToNetworkCore(temp_key.c_str(), temp_value.c_str(), val_len, false);
        }
      }

      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "ready to send back to client!" << std::endl;
      }
      for (int j = 0; j < int(blocks_idx_ptr->size()); j++)
      {
        int idx = (*blocks_idx_ptr)[j] - start_id;
        memcpy(data[idx], (*blocks_ptr)[j].data(), block_size);
      }

      std::string value;
      for (int j = 0; j < block_num; j++)
      {
        value += std::string(data[j]);
      }

      if (IF_DEBUG)
      {
        std::cout << "\033[1;31m[Proxy" << m_self_cluster_id << "][GET]"
                  << "send " << object_key << " to client with length of " << value.size() << "\033[0m" << std::endl;
      }

      // send to the client
      asio::error_code error;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(clientip, std::to_string(clientport));
      asio::ip::tcp::socket sock_data(io_context);
      asio::connect(sock_data, endpoints);

      asio::write(sock_data, asio::buffer(object_key, object_key.size()), error);
      asio::write(sock_data, asio::buffer(value, value_size_bytes), error);
      asio::error_code ignore_ec;
      sock_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
      sock_data.close(ignore_ec);
    };
    try
    {
      if (IF_DEBUG)
      {
        std::cout << "[Proxy] Handle get!" << std::endl;
      }
      std::thread my_thread(decode_and_get);
      my_thread.detach();
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }

  // delete
  grpc::Status ProxyImpl::deleteBlock(
      grpc::ServerContext *context,
      const proxy_proto::NodeAndBlock *node_and_block,
      proxy_proto::DelReply *response)
  {
    std::vector<std::string> blocks_id;
    std::vector<std::string> nodes_ip_port;
    int stripe_id = node_and_block->stripe_id();
    for (int i = 0; i < node_and_block->blockkeys_size(); i++)
    {
      blocks_id.push_back(node_and_block->blockkeys(i));
      std::string ip_port = node_and_block->datanodeip(i) + ":" + std::to_string(node_and_block->datanodeport(i));
      nodes_ip_port.push_back(ip_port);
    }
    auto delete_blocks = [this, blocks_id, stripe_id, nodes_ip_port]() mutable
    {
      auto request_and_delete = [this](std::string block_key, std::string node_ip_port)
      {
        bool ret = DelInDatanode(block_key, node_ip_port);
        if (!ret)
        {
          std::cout << "Delete value no return!" << std::endl;
          return;
        }
      };
      try
      {
        std::vector<std::thread> senders;
        for (int j = 0; j < int(blocks_id.size()); j++)
        {
          senders.push_back(std::thread(request_and_delete, blocks_id[j], nodes_ip_port[j]));
        }

        for (int j = 0; j < int(senders.size()); j++)
        {
          senders[j].join();
        }

        grpc::ClientContext c_context;
        coordinator_proto::CommitAbortStripe commit_abort_stripe;
        coordinator_proto::ReplyFromCoordinator rep;
        ECProject::OpperateType opp = DEL;
        commit_abort_stripe.set_opp(opp);
        commit_abort_stripe.set_ifcommitmetadata(true);
        commit_abort_stripe.set_stripe_id(stripe_id);
        grpc::Status stat;
        stat = m_coordinator_ptr->reportCommitAbort(&c_context, commit_abort_stripe, &rep);
      }
      catch (const std::exception &e)
      {
        std::cout << "exception" << std::endl;
        std::cerr << e.what() << '\n';
      }
    };
    try
    {
      std::thread my_thread(delete_blocks);
      my_thread.detach();
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }

  // check
  grpc::Status ProxyImpl::checkStep(
      grpc::ServerContext *context,
      const proxy_proto::AskIfSuccess *step,
      proxy_proto::RepIfSuccess *response)
  {
    std::unique_lock<std::mutex> lck(m_mutex);
    // int idx = step->step();
    if (IF_DEBUG)
    {
      std::cout << "\033[1;34m[Main Proxy" << m_self_cluster_id << "] Ask if repaired!" << "\033[0m\n";
    }
    while(!m_if_repaired)
    {
      cv.wait(lck);
    }
    response->set_ifsuccess(true);
    return grpc::Status::OK;
  }

} // namespace ECProject
