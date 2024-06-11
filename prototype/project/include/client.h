#ifndef CLIENT_H
#define CLIENT_H

#ifdef BAZEL_BUILD
#include "src/proto/coordinator.grpc.pb.h"
#else
#include "coordinator.grpc.pb.h"
#endif

#include "meta_definition.h"
#include <grpcpp/grpcpp.h>
#include <asio.hpp>
namespace ECProject
{
  class Client
  {
  public:
    Client(std::string ClientIP, int ClientPort, std::string CoordinatorIpPort) : m_coordinatorIpPort(CoordinatorIpPort),
                                                                                  m_clientIPForGet(ClientIP),
                                                                                  m_clientPortForGet(ClientPort),
                                                                                  acceptor(io_context, asio::ip::tcp::endpoint(asio::ip::address::from_string(ClientIP.c_str()), m_clientPortForGet))
    {
      auto channel = grpc::CreateChannel(m_coordinatorIpPort, grpc::InsecureChannelCredentials());
      m_coordinator_ptr = coordinator_proto::coordinatorService::NewStub(channel);
    }
    std::string sayHelloToCoordinatorByGrpc(std::string hello);
    bool SetParameterByGrpc(ECSchema input_ecschema);
    // set, n files for each set
    bool set(std::string key, std::string value, std::vector<std::string> &object_keys, std::vector<int> &object_sizes, std::vector<int> &object_accessrates, float x, int g_m);
    // get
    bool get(std::string object_key, std::string &value);
    // delete
    bool delete_stripe(int stripe_id);
    bool delete_all_stripes();
    // repair
    int nodes_repair(std::vector<int> &failed_node_list, double &cost, int &cross_cluster_num);
    int blocks_repair(std::vector<int> &failed_block_list, int stripe_id, double &cost, int &cross_cluster_num);

  private:
    std::unique_ptr<coordinator_proto::coordinatorService::Stub> m_coordinator_ptr;
    std::string m_coordinatorIpPort;
    std::string m_clientIPForGet;
    int m_clientPortForGet;
    asio::io_context io_context;
    asio::ip::tcp::acceptor acceptor;
  };

} // namespace ECProject

#endif // CLIENT_H