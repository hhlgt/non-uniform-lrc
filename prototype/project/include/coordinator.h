#ifndef COORDINATOR_H
#define COORDINATOR_H
#include "coordinator.grpc.pb.h"
#include "proxy.grpc.pb.h"
#include <grpc++/create_channel.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <meta_definition.h>
#include <mutex>
#include <thread>
#include <cmath>
#include <condition_variable>
#include "lrc.h"
// #define IF_DEBUG true
#define IF_DEBUG false
namespace ECProject
{
  class CoordinatorImpl final
      : public coordinator_proto::coordinatorService::Service
  {
  public:
    CoordinatorImpl()
    {
      m_cur_cluster_id = 0;
      m_cur_stripe_id = 0;
    }
    ~CoordinatorImpl(){};
    grpc::Status setParameter(
        grpc::ServerContext *context,
        const coordinator_proto::Parameter *parameter,
        coordinator_proto::RepIfSetParaSuccess *setParameterReply) override;
    grpc::Status sayHelloToCoordinator(
        grpc::ServerContext *context,
        const coordinator_proto::RequestToCoordinator *helloRequestToCoordinator,
        coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator) override;
    grpc::Status checkalive(
        grpc::ServerContext *context,
        const coordinator_proto::RequestToCoordinator *helloRequestToCoordinator,
        coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator) override;
    // set
    grpc::Status uploadOriginKeyValue(
        grpc::ServerContext *context,
        const coordinator_proto::RequestProxyIPPort *keyValueSize,
        coordinator_proto::ReplyProxyIPPort *proxyIPPort) override;
    grpc::Status reportCommitAbort(
        grpc::ServerContext *context,
        const coordinator_proto::CommitAbortStripe *commit_abortkey,
        coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator) override;
    grpc::Status checkCommitAbort(
        grpc::ServerContext *context,
        const coordinator_proto::AskIfSuccess *stripe_id,
        coordinator_proto::RepIfSuccess *reply) override;
    // get
    grpc::Status getValue(
        grpc::ServerContext *context,
        const coordinator_proto::KeyAndClientIP *keyClient,
        coordinator_proto::RepIfGetSuccess *getReplyClient) override;
    // delete
    grpc::Status delByStripe(
        grpc::ServerContext *context,
        const coordinator_proto::StripeIdFromClient *stripeid,
        coordinator_proto::RepIfDeling *delReplyClient) override;
    // repair
    grpc::Status requestRepair(
        grpc::ServerContext *context,
        const coordinator_proto::FailureInfo *failures,
        coordinator_proto::RepIfRepaired *repairReplyClient) override;
    // other
    grpc::Status listStripes(
        grpc::ServerContext *context,
        const coordinator_proto::RequestToCoordinator *req,
        coordinator_proto::RepStripeIds *listReplyClient) override;

    bool init_clusterinfo(std::string m_clusterinfo_path);
    bool init_proxyinfo();
    
    // placement
    void update_stripe_info_in_node(bool add_or_sub, int t_node_id, int stripe_id);
    int generate_placement_for_LRC(int stripe_id, int block_size);
    
    // repair
    void check_out_failures(bool isblock, int stripe_id, std::shared_ptr<std::vector<int>> failureinfo,
                            std::shared_ptr<std::map<int, std::vector<Block *>>> failure_map,
                            std::shared_ptr<std::map<int, ECProject::FailureType>> failures_type);
    void request_repair(bool isblock, int stripe_id, std::shared_ptr<std::vector<int>> blocks_or_nodes, coordinator_proto::RepIfRepaired *repairReplyClient);
    bool generate_repair_plan_for_single_block_LRC(std::vector<proxy_proto::mainRepairPlan> &main_repair, std::vector<std::vector<proxy_proto::helpRepairPlan>> &help_repair, int stripe_id, std::vector<Block *> &failed_blocks);
    bool generate_repair_plan_for_multi_blocks_LRC(std::vector<proxy_proto::mainRepairPlan> &main_repair, std::vector<std::vector<proxy_proto::helpRepairPlan>> &help_repair, int stripe_id, std::vector<Block *> &failed_blocks);
    void simulation_repair(std::vector<proxy_proto::mainRepairPlan> &main_repair, int &cross_cluster_num);

  private:
    std::mutex m_mutex;
    std::condition_variable cv;
    int m_cur_cluster_id = 0;
    int m_cur_stripe_id = 0;
    std::map<std::string, std::unique_ptr<proxy_proto::proxyService::Stub>>
        m_proxy_ptrs;
    ECSchema m_encode_parameters;
    std::unordered_map<std::string, int> m_object_commit_table; // key to stripe
    std::unordered_map<std::string, int> m_object_updating_table;
    std::vector<int> m_stripe_commit_table;
    std::vector<int> m_stripe_updating_table;
    std::map<int, Cluster> m_cluster_table;
    std::map<int, Node> m_node_table;
    std::map<int, Stripe> m_stripe_table;
    int m_num_of_Clusters;
    int m_num_of_Nodes_in_Cluster;
  };

  class Coordinator
  {
  public:
    Coordinator(
        std::string m_coordinator_ip_port,
        std::string m_clusterinfo_path)
        : m_coordinator_ip_port{m_coordinator_ip_port},
          m_clusterinfo_path{m_clusterinfo_path}
    {
      m_coordinatorImpl.init_clusterinfo(m_clusterinfo_path);
      m_coordinatorImpl.init_proxyinfo();
    };
    // Coordinator
    void Run()
    {
      grpc::EnableDefaultHealthCheckService(true);
      grpc::reflection::InitProtoReflectionServerBuilderPlugin();
      grpc::ServerBuilder builder;
      std::string server_address(m_coordinator_ip_port);
      builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
      builder.RegisterService(&m_coordinatorImpl);
      std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
      std::cout << "Server listening on " << server_address << std::endl;
      server->Wait();
    }

  private:
    std::string m_coordinator_ip_port;
    std::string m_clusterinfo_path;
    ECProject::CoordinatorImpl m_coordinatorImpl;
  };
} // namespace ECProject

#endif