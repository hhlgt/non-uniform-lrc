#include "client.h"
#include "coordinator.grpc.pb.h"

#include <asio.hpp>
namespace ECProject
{
  std::string Client::sayHelloToCoordinatorByGrpc(std::string hello)
  {
    coordinator_proto::RequestToCoordinator request;
    request.set_name(hello);
    coordinator_proto::ReplyFromCoordinator reply;
    grpc::ClientContext context;
    grpc::Status status = m_coordinator_ptr->sayHelloToCoordinator(&context, request, &reply);
    if (status.ok())
    {
      return reply.message();
    }
    else
    {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }
  // grpc, set the parameters stored in the variable of m_encode_parameters in coordinator
  bool Client::SetParameterByGrpc(ECSchema input_ecschema)
  {
    int k = input_ecschema.k_datablock;
    int l = input_ecschema.l_localparityblock;
    int g_m = input_ecschema.g_m_globalparityblock;
    int r = input_ecschema.r_avggroupsize;
    EncodeType encodetype = input_ecschema.encodetype;
    if((encodetype == Azu_LRC && r != (k + l - 1) / l) && r != (k + g_m + l - 1) / l)
    {
      std::cout << "Set parameters failed! Illegal parameters!" << std::endl;
      exit(0);
    }
    coordinator_proto::Parameter parameter;
    parameter.set_testtype((int)input_ecschema.testtype);
    parameter.set_partial_decoding((int)input_ecschema.partial_decoding);
    parameter.set_encodetype(encodetype);
    parameter.set_s_placementtype(input_ecschema.s_placementtype);
    parameter.set_n_fileperstripe(input_ecschema.n_fileperstripe);
    parameter.set_k_datablock(k);
    parameter.set_g_m_globalparityblock(g_m);
    parameter.set_r_avggroupsize(r);
    parameter.set_x_storageoverhead(input_ecschema.x_storageoverhead);
    parameter.set_s_blocksizebytes(input_ecschema.s_blocksizebytes);
    if(encodetype == NU_LRC && int(input_ecschema.cp.size()) > 0)
    {
      if(input_ecschema.n_fileperstripe != int(input_ecschema.cp.size()) - 1 && encodetype == NU_LRC)
      {
        std::cout << "Set parameters failed! Illegal parameters! (Non-uniform)" << std::endl;
        exit(0);
      }
      for(int i = 0; i <= input_ecschema.n_fileperstripe; i++)
      {
        parameter.add_cp_ki((int)input_ecschema.cp[i].ki);
        parameter.add_cp_ri((int)input_ecschema.cp[i].ri);
      }
    }
    grpc::ClientContext context;
    coordinator_proto::RepIfSetParaSuccess reply;
    grpc::Status status = m_coordinator_ptr->setParameter(&context, parameter, &reply);
    if (status.ok())
    {
      return reply.ifsetparameter();
    }
    else
    {
      std::cout << status.error_code() << ": " << status.error_message() << std::endl;
      return false;
    }
  }
  /*
    Function: set
    1. send the set request including the information of check_key and total_valuesize, object_key and object_size of n files to the coordinator
    2. get the address of proxy
    3. send the value to the proxy by socket
  */
  bool Client::set(std::string key, std::string value, std::vector<std::string> &object_keys, std::vector<int> &object_sizes, std::vector<int> &object_accessrates, float x, int g_m)
  {
    grpc::ClientContext get_proxy_ip_port;
    coordinator_proto::RequestProxyIPPort request;
    coordinator_proto::ReplyProxyIPPort reply;
    request.set_key(key);
    request.set_valuesizebytes(value.size());
    int n = int(object_keys.size());
    for(int i = 0; i < n; i++)
    {
      request.add_object_keys(object_keys[i]);
      request.add_object_sizebytes(object_sizes[i]);
    }
    if(int(object_accessrates.size()) > 0)
    {
      for(int i = 0; i < n; i++)
      {
        request.add_object_accessrates(object_accessrates[i]);
      }
    }
    request.set_x_storageoverhead(x);
    request.set_g_m_globalparityblock(g_m);

    grpc::Status status = m_coordinator_ptr->uploadOriginKeyValue(&get_proxy_ip_port, request, &reply);
    if (!status.ok())
    {
      std::cout << "[SET] upload data failed!" << std::endl;
      return false;
    }
    else
    {
      std::string proxy_ip = reply.proxyip();
      int proxy_port = reply.proxyport();
      std::cout << "[SET] Send " << key << " to proxy_address:" << proxy_ip << ":" << proxy_port << std::endl;
      // read to send the value
      asio::io_context io_context;
      asio::error_code error;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::resolver::results_type endpoints =
          resolver.resolve(proxy_ip, std::to_string(proxy_port));
      asio::ip::tcp::socket sock_data(io_context);
      asio::connect(sock_data, endpoints);

      asio::write(sock_data, asio::buffer(key, key.size()), error);
      asio::write(sock_data, asio::buffer(value, value.size()), error);
      asio::error_code ignore_ec;
      sock_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
      sock_data.close(ignore_ec);

      // check if metadata is saved successfully
      int stripe_id = reply.stripe_id();
      grpc::ClientContext check_commit;
      coordinator_proto::AskIfSuccess request;
      request.set_stripe_id(stripe_id);
      coordinator_proto::RepIfSuccess reply;
      grpc::Status status;
      status = m_coordinator_ptr->checkCommitAbort(&check_commit, request, &reply);
      if (status.ok())
      {
        if (reply.ifcommit())
        {
          return true;
        }
        else
        {
          std::cout << "[SET] " << key << " not commit!!!!!" << std::endl;
        }
      }
      else
      {
        std::cout << "[SET] " << key << " Fail to check!!!!!" << std::endl;
      }
    }
    return false;
  }
  /*
    Function: get
    1. send the get request including the information of object_key and clientipport to the coordinator
    2. accept the value transferred from the proxy
  */
  bool Client::get(std::string object_key, std::string &value)
  {
    grpc::ClientContext context;
    coordinator_proto::KeyAndClientIP request;
    request.set_object_key(object_key);
    request.set_clientip(m_clientIPForGet);
    request.set_clientport(m_clientPortForGet);
    // request
    coordinator_proto::RepIfGetSuccess reply;
    grpc::Status status = m_coordinator_ptr->getValue(&context, request, &reply);
    asio::ip::tcp::socket socket_data(io_context);
    int value_size = reply.valuesizebytes();
    acceptor.accept(socket_data);
    asio::error_code error;
    std::vector<char> buf_key(object_key.size());
    std::vector<char> buf(value_size);
    // read from socket
    size_t len = asio::read(socket_data, asio::buffer(buf_key, object_key.size()), error);
    int flag = 1;
    for (int i = 0; i < int(object_key.size()); i++)
    {
      if (object_key[i] != buf_key[i])
      {
        flag = 0;
      }
    }
    if (flag)
    {
      len = asio::read(socket_data, asio::buffer(buf, value_size), error);
    }
    else
    {
      std::cout << "[GET] key not matches!" << std::endl;
    }
    asio::error_code ignore_ec;
    socket_data.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
    socket_data.close(ignore_ec);
    if (flag)
    {
      std::cout << "[GET] get key: " << buf_key.data() << " ,valuesize: " << len << std::endl;
    }
    value = std::string(buf.data(), buf.size());
    return true;
  }

  bool Client::delete_stripe(int stripe_id)
  {
    grpc::ClientContext context;
    coordinator_proto::StripeIdFromClient request;
    request.set_stripe_id(stripe_id);
    coordinator_proto::RepIfDeling reply;
    grpc::Status status = m_coordinator_ptr->delByStripe(&context, request, &reply);
    if (status.ok())
    {
      if (reply.ifdeling())
      {
        std::cout << "[DEL] deleting Stripe " << stripe_id << std::endl;
      }
      else
      {
        std::cout << "[DEL] delete failed!" << std::endl;
      }
    }
    // check if metadata is saved successfully
    grpc::ClientContext check_commit;
    coordinator_proto::AskIfSuccess req;
    req.set_stripe_id(stripe_id);
    coordinator_proto::RepIfSuccess rep;
    grpc::Status stat;
    stat = m_coordinator_ptr->checkCommitAbort(&check_commit, req, &rep);
    if (stat.ok())
    {
      if (rep.ifcommit())
      {
        return true;
      }
      else
      {
        std::cout << "[DEL] Stripe" << stripe_id << " not delete!!!!!";
      }
    }
    else
    {
      std::cout << "[DEL] Stripe" << stripe_id << " Fail to check!!!!!";
    }
    return false;
  }

  bool Client::delete_all_stripes()
  {
    grpc::ClientContext context;
    coordinator_proto::RepStripeIds rep;
    coordinator_proto::RequestToCoordinator req;
    grpc::Status status = m_coordinator_ptr->listStripes(&context, req, &rep);
    if (status.ok())
    {
      std::cout << "Deleting all stripes!" << std::endl;
      for (int i = 0; i < int(rep.stripe_ids_size()); i++)
      {
        delete_stripe(rep.stripe_ids(i));
      }
      return true;
    }
    return false;
  }
  /*
    Function: node repair
    1. send the repair request including the information of failed_node_list to the coordinator
  */
  int Client::nodes_repair(std::vector<int> &failed_node_list, double &cost, int &cross_cluster_num)
  {
    
    grpc::ClientContext context;
    coordinator_proto::FailureInfo request;
    request.set_isblock(false);
    request.set_stripe_id(-1);
    for (int i = 0; i < int(failed_node_list.size()); i++)
    {
      request.add_blocks_or_nodes(failed_node_list[i]);
    }
    coordinator_proto::RepIfRepaired reply;
    grpc::Status status = m_coordinator_ptr->requestRepair(&context, request, &reply);
    int failed_stripe_num = 0;
    if (status.ok())
    {
      if (reply.ifrepaired())
      {
        cost = reply.rc();
        cross_cluster_num = reply.cross_cluster_num();
        failed_stripe_num = reply.failed_stripe_num();
      }
      else
      {
        std::cout << "[Repair] repair failed!" << std::endl;
      }
    }
    return failed_stripe_num;
  }
  /*
    Function: block repair
    1. send the repair request including the information of failed_block_list to the coordinator
  */
  int Client::blocks_repair(std::vector<int> &failed_block_list, int stripe_id, double &cost, int &cross_cluster_num)
  {
    grpc::ClientContext context;
    coordinator_proto::FailureInfo request;
    request.set_isblock(true);
    request.set_stripe_id(stripe_id);
    for (int i = 0; i < int(failed_block_list.size()); i++)
    {
      request.add_blocks_or_nodes(failed_block_list[i]);
    }
    coordinator_proto::RepIfRepaired reply;
    grpc::Status status = m_coordinator_ptr->requestRepair(&context, request, &reply);
    int failed_stripe_num = 0;
    if (status.ok())
    {
      if (reply.ifrepaired())
      {
        cost = reply.rc();
        cross_cluster_num = reply.cross_cluster_num();
        failed_stripe_num = reply.failed_stripe_num();
      }
      else
      {
        std::cout << "[Repair] repair failed!" << std::endl;
      }
    }
    return failed_stripe_num;
  }
} // namespace ECProject