#include "coordinator.h"
#include "tinyxml2.h"
#include <random>
#include <algorithm>
#include <unistd.h>
#include <sys/time.h>

namespace ECProject
{
    bool CoordinatorImpl::init_proxyinfo()
    {
        for (auto cur = m_cluster_table.begin(); cur != m_cluster_table.end(); cur++)
        {
            std::string proxy_ip_and_port = cur->second.proxy_ip + ":" + std::to_string(cur->second.proxy_port);
            auto _stub = proxy_proto::proxyService::NewStub(grpc::CreateChannel(proxy_ip_and_port, grpc::InsecureChannelCredentials()));
            proxy_proto::CheckaliveCMD Cmd;
            proxy_proto::RequestResult result;
            grpc::ClientContext clientContext;
            Cmd.set_name("coordinator");
            grpc::Status status;
            status = _stub->checkalive(&clientContext, Cmd, &result);
            if (status.ok())
            {
                std::cout << "[Proxy Check] ok from " << proxy_ip_and_port << std::endl;
            }
            else
            {
                std::cout << "[Proxy Check] failed to connect " << proxy_ip_and_port << std::endl;
            }
            m_proxy_ptrs.insert(std::make_pair(proxy_ip_and_port, std::move(_stub)));
        }
        return true;
    }

    bool CoordinatorImpl::init_clusterinfo(std::string m_clusterinfo_path)
    {
        std::cout << "Cluster_information_path:" << m_clusterinfo_path << std::endl;
        tinyxml2::XMLDocument xml;
        xml.LoadFile(m_clusterinfo_path.c_str());
        tinyxml2::XMLElement *root = xml.RootElement();
        int node_id = 0;
        m_num_of_Clusters = 0;
        for (tinyxml2::XMLElement *cluster = root->FirstChildElement(); cluster != nullptr; cluster = cluster->NextSiblingElement())
        {
            std::string cluster_id(cluster->Attribute("id"));
            std::string proxy(cluster->Attribute("proxy"));
            std::cout << "cluster_id: " << cluster_id << " , proxy: " << proxy << std::endl;
            Cluster t_cluster;
            m_cluster_table[std::stoi(cluster_id)] = t_cluster;
            m_cluster_table[std::stoi(cluster_id)].cluster_id = std::stoi(cluster_id);
            auto pos = proxy.find(':');
            m_cluster_table[std::stoi(cluster_id)].proxy_ip = proxy.substr(0, pos);
            m_cluster_table[std::stoi(cluster_id)].proxy_port = std::stoi(proxy.substr(pos + 1, proxy.size()));
            int nodespercluster = 0;
            for (tinyxml2::XMLElement *node = cluster->FirstChildElement()->FirstChildElement(); node != nullptr; node = node->NextSiblingElement())
            {
                std::string node_uri(node->Attribute("uri"));
                std::cout << "____node: " << node_uri << std::endl;
                m_cluster_table[std::stoi(cluster_id)].nodes.push_back(node_id);
                m_node_table[node_id].node_id = node_id;
                auto pos = node_uri.find(':');
                m_node_table[node_id].node_ip = node_uri.substr(0, pos);
                m_node_table[node_id].node_port = std::stoi(node_uri.substr(pos + 1, node_uri.size()));
                m_node_table[node_id].map2cluster = std::stoi(cluster_id);
                node_id++;
                nodespercluster++;
            }
            m_num_of_Nodes_in_Cluster = nodespercluster;
            m_num_of_Clusters++;
        }
        return true;
    }

    void CoordinatorImpl::update_stripe_info_in_node(bool add_or_sub, int t_node_id, int stripe_id)
    {
        int stripe_block_num = 0;
        if (m_node_table[t_node_id].stripes.find(stripe_id) != m_node_table[t_node_id].stripes.end())
        {
            stripe_block_num = m_node_table[t_node_id].stripes[stripe_id];
        }
        if (add_or_sub)
        {
            m_node_table[t_node_id].stripes[stripe_id] = stripe_block_num + 1;
        }
        else
        {
            if (stripe_block_num == 1)
            {
                m_node_table[t_node_id].stripes.erase(stripe_id);
            }
            else
            {
                m_node_table[t_node_id].stripes[stripe_id] = stripe_block_num - 1;
            }
        }
    }

} // namespace ECProject