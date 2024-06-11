#include "coordinator.h"
#include "tinyxml2.h"
#include <random>
#include <unistd.h>
#include "lrc.h"
#include <sys/time.h>

template <typename T>
inline T ceil(T const &A, T const &B)
{
    return T((A + B - 1) / B);
};

inline int rand_num(int range)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis(0, range - 1);
    int num = dis(gen);
    return num;
};

namespace ECProject
{
    grpc::Status CoordinatorImpl::setParameter(
        grpc::ServerContext *context,
        const coordinator_proto::Parameter *parameter,
        coordinator_proto::RepIfSetParaSuccess *setParameterReply)
    {
        std::vector<fp> n_coding_parameters;
        int n_1 = parameter->cp_ki_size();
        if(n_1 > 0)
        {
            for(int i = 0; i < n_1; i++)
            {
                fp tmp;
                tmp.ki = (ushort)parameter->cp_ki(i);
                tmp.ri = (ushort)parameter->cp_ri(i);
                n_coding_parameters.push_back(tmp);
            }
        }
        ECSchema system_metadata(parameter->testtype(),
                                parameter->partial_decoding(),
                                (ECProject::EncodeType)parameter->encodetype(),
                                (ECProject::PlacementType)parameter->s_placementtype(),
                                parameter->n_fileperstripe(),
                                parameter->k_datablock(),
                                parameter->r_avggroupsize(),
                                parameter->g_m_globalparityblock(),
                                parameter->x_storageoverhead(),
                                parameter->s_blocksizebytes(),
                                n_coding_parameters);
        m_encode_parameters = system_metadata;
        setParameterReply->set_ifsetparameter(true);
        m_cur_cluster_id = 0;
        m_cur_stripe_id = 0;
        m_object_commit_table.clear();
        m_object_updating_table.clear();
        m_stripe_commit_table.clear();
        m_stripe_updating_table.clear();
        for (auto it = m_cluster_table.begin(); it != m_cluster_table.end(); it++)
        {
            Cluster &t_cluster = it->second;
            t_cluster.blocks.clear();
            t_cluster.stripes.clear();
        }
        for (auto it = m_node_table.begin(); it != m_node_table.end(); it++)
        {
            Node &t_node = it->second;
            t_node.stripes.clear();
        }
        m_stripe_table.clear();
        std::cout << "setParameter success" << std::endl;
        return grpc::Status::OK;
    }

    grpc::Status CoordinatorImpl::sayHelloToCoordinator(
        grpc::ServerContext *context,
        const coordinator_proto::RequestToCoordinator *helloRequestToCoordinator,
        coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator)
    {
        std::string prefix("Hello ");
        helloReplyFromCoordinator->set_message(prefix + helloRequestToCoordinator->name());
        std::cout << prefix + helloRequestToCoordinator->name() << std::endl;
        return grpc::Status::OK;
    }

    grpc::Status CoordinatorImpl::uploadOriginKeyValue(
        grpc::ServerContext *context,
        const coordinator_proto::RequestProxyIPPort *keyValueSize,
        coordinator_proto::ReplyProxyIPPort *proxyIPPort)
    {
        ECProject::EncodeType encode_type = (ECProject::EncodeType)m_encode_parameters.encodetype;
        int testtype = m_encode_parameters.testtype;
        std::string key = keyValueSize->key();
        int valuesizebytes = keyValueSize->valuesizebytes();
        int object_num = keyValueSize->object_keys_size();
        float x = keyValueSize->x_storageoverhead();
        int g_m = keyValueSize->g_m_globalparityblock();

        if(!testtype && object_num != m_encode_parameters.n_fileperstripe)
        {
            std::cout << "[ERROR] Illegal SET!" << std::endl;
        }

        std::vector<std::string> object_keys;
        std::vector<int> object_sizebytes;
        std::vector<int> object_accessrates;
        for(int i = 0; i < object_num; i++)
        {
            object_keys.push_back(keyValueSize->object_keys(i));
            object_sizebytes.push_back(keyValueSize->object_sizebytes(i));   
        }
        m_mutex.lock();
        for(int i = 0; i < object_num; i++)
        {
            m_object_commit_table.erase(object_keys[i]);
        }
        m_mutex.unlock();

        std::vector<fp> stripe_coding_paras;
        int k, r, l;
        int block_size = m_encode_parameters.s_blocksizebytes;
        // ECProject::PlacementType placement_type = m_encode_parameters.s_placementtype;
        if(encode_type == NU_LRC)
        {
            if(testtype)
            {
                for(int i = 0; i < object_num; i++)
                {
                    object_accessrates.push_back(keyValueSize->object_accessrates(i));
                }
                generate_coding_parameters_for_stripe_NU_LRC(k, r, l, g_m, x, block_size, stripe_coding_paras, object_sizebytes, object_accessrates);
            }
            else
            {
                stripe_coding_paras = m_encode_parameters.cp;
                k = m_encode_parameters.k_datablock;
                r = m_encode_parameters.r_avggroupsize;
                l = m_encode_parameters.l_localparityblock;
            }
        }
        else if(encode_type == U_LRC)
        {
            if(testtype)
            {
                generate_coding_parameters_for_stripe_U_LRC(k, r, l, g_m, x, block_size, object_sizebytes);
            }
            else
            {
                k = m_encode_parameters.k_datablock;
                r = m_encode_parameters.r_avggroupsize;
                l = m_encode_parameters.l_localparityblock;
            }
        }
        else
        {
            if(testtype)
            {
                generate_coding_parameters_for_stripe_Azu_LRC(k, r, l, g_m, x, block_size, object_sizebytes);
            }
            else
            {
                k = m_encode_parameters.k_datablock;
                r = m_encode_parameters.r_avggroupsize;
                l = m_encode_parameters.l_localparityblock;
            }
        }

        proxy_proto::ObjectAndPlacement object_placement;
        Stripe t_stripe;
        t_stripe.stripe_id = m_cur_stripe_id++;
        t_stripe.k = k;
        t_stripe.r = r;
        t_stripe.g_m = g_m;
        object_placement.set_key(key);
        object_placement.set_valuesizebyte(valuesizebytes);
        object_placement.set_k(k);
        object_placement.set_g_m(g_m);
        object_placement.set_r(r);
        object_placement.set_l(l);
        object_placement.set_encode_type((int)encode_type);
        object_placement.set_block_size(block_size);
        if(encode_type == NU_LRC)
        {
            for(fp tmp : stripe_coding_paras)
            {
                object_placement.add_ki((int)tmp.ki);
                object_placement.add_ri((int)tmp.ri);
                t_stripe.cp.push_back(tmp);
            }
        }

        for(int i = 0; i < object_num; i++){
            t_stripe.object_keys.push_back(object_keys[i]);
            t_stripe.object_sizes.push_back(object_sizebytes[i]);
        }
        m_stripe_table[t_stripe.stripe_id] = t_stripe;
        
        if(IF_DEBUG)
        {
            std::cout << "Generate data placement scheme" << std::endl;
        }
        int s_cluster_id = generate_placement_for_LRC(t_stripe.stripe_id, block_size);;

        Stripe &stripe = m_stripe_table[t_stripe.stripe_id];
        object_placement.set_stripe_id(stripe.stripe_id);
        for (int i = 0; i < int(stripe.blocks.size()); i++)
        {
            object_placement.add_datanodeip(m_node_table[stripe.blocks[i]->map2node].node_ip);
            object_placement.add_datanodeport(m_node_table[stripe.blocks[i]->map2node].node_port);
            object_placement.add_blockkeys(stripe.blocks[i]->block_key);
        }

        grpc::ClientContext cont;
        proxy_proto::SetReply set_reply;
        std::string selected_proxy_ip = m_cluster_table[s_cluster_id].proxy_ip;
        int selected_proxy_port = m_cluster_table[s_cluster_id].proxy_port;
        std::string chosen_proxy = selected_proxy_ip + ":" + std::to_string(selected_proxy_port);
        grpc::Status status = m_proxy_ptrs[chosen_proxy]->encodeAndSetObject(&cont, object_placement, &set_reply);
        proxyIPPort->set_proxyip(selected_proxy_ip);
        proxyIPPort->set_proxyport(selected_proxy_port + 1); // use another port to accept data
        proxyIPPort->set_stripe_id(t_stripe.stripe_id);
        if (status.ok())
        {
            m_mutex.lock();
            for(int i = 0; i < object_num; i++)
            {
                m_object_updating_table[object_keys[i]] = t_stripe.stripe_id;
            }
            m_stripe_updating_table.push_back(t_stripe.stripe_id);
            m_mutex.unlock();
        }
        else
        {
            std::cout << "[SET] Send object placement failed!" << std::endl;
        }

        return grpc::Status::OK;
    }

    grpc::Status CoordinatorImpl::getValue(
        grpc::ServerContext *context,
        const coordinator_proto::KeyAndClientIP *keyClient,
        coordinator_proto::RepIfGetSuccess *getReplyClient)
    {
        try
        {
            std::string object_key = keyClient->object_key();
            std::string client_ip = keyClient->clientip();
            int client_port = keyClient->clientport();
            int map2stripe = -1;
            m_mutex.lock();
            map2stripe = m_object_commit_table.at(object_key);
            m_mutex.unlock();

            grpc::ClientContext decode_and_get;
            proxy_proto::GetObject object_info;
            grpc::Status status;
            proxy_proto::GetReply get_reply;

            object_info.set_object_key(object_key);
            object_info.set_block_size(m_encode_parameters.s_blocksizebytes);
            object_info.set_clientip(client_ip);
            object_info.set_clientport(client_port);
            Stripe &t_stripe = m_stripe_table[map2stripe];
            int block_num = 0, start_id = 1000000;
            std::unordered_set<int> t_cluster_set;
            for (int i = 0; i < int(t_stripe.blocks.size()); i++)
            {
                if (t_stripe.blocks[i]->map2key == object_key)
                {
                    block_num++;
                    object_info.add_datanodeip(m_node_table[t_stripe.blocks[i]->map2node].node_ip);
                    object_info.add_datanodeport(m_node_table[t_stripe.blocks[i]->map2node].node_port);
                    object_info.add_blockkeys(t_stripe.blocks[i]->block_key);
                    object_info.add_blockids(t_stripe.blocks[i]->block_id);
                    t_cluster_set.insert(t_stripe.blocks[i]->map2cluster);
                    if(start_id > t_stripe.blocks[i]->block_id)
                    {
                        start_id = t_stripe.blocks[i]->block_id;
                    }
                }
            }
            object_info.set_block_num(block_num);
            object_info.set_start_id(start_id);
            getReplyClient->set_ifgetsuccess(true);
            getReplyClient->set_valuesizebytes(block_num * m_encode_parameters.s_blocksizebytes);
            // randomly select a cluster
            int idx = rand_num(int(t_cluster_set.size()));
            int r_cluster_id = *(std::next(t_cluster_set.begin(), idx));
            std::string chosen_proxy = m_cluster_table[r_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[r_cluster_id].proxy_port);
            status = m_proxy_ptrs[chosen_proxy]->decodeAndGetObject(&decode_and_get, object_info, &get_reply);
            if (status.ok())
            {
                std::cout << "[GET] getting value of " << object_key << std::endl;
            }
        }
        catch (std::exception &e)
        {
            std::cout << "getValue exception" << std::endl;
            std::cout << e.what() << std::endl;
        }
        return grpc::Status::OK;
    }

    grpc::Status CoordinatorImpl::delByStripe(
        grpc::ServerContext *context,
        const coordinator_proto::StripeIdFromClient *stripeid,
        coordinator_proto::RepIfDeling *delReplyClient)
    {
        try
        {
            int t_stripe_id = stripeid->stripe_id();
            m_mutex.lock();
            m_stripe_updating_table.push_back(t_stripe_id);
            m_mutex.unlock();

            grpc::ClientContext context;
            proxy_proto::NodeAndBlock node_block;
            grpc::Status status;
            proxy_proto::DelReply del_reply;
            Stripe &t_stripe = m_stripe_table[t_stripe_id];
            std::unordered_set<int> t_cluster_set;
            for (int i = 0; i < int(t_stripe.blocks.size()); i++)
            {
                if (t_stripe.blocks[i]->map2stripe == t_stripe_id)
                {
                    node_block.add_datanodeip(m_node_table[t_stripe.blocks[i]->map2node].node_ip);
                    node_block.add_datanodeport(m_node_table[t_stripe.blocks[i]->map2node].node_port);
                    node_block.add_blockkeys(t_stripe.blocks[i]->block_key);
                    t_cluster_set.insert(t_stripe.blocks[i]->map2cluster);
                }
            }
            node_block.set_stripe_id(t_stripe_id);
            // randomly select a cluster
            int idx = rand_num(int(t_cluster_set.size()));
            int r_cluster_id = *(std::next(t_cluster_set.begin(), idx));
            std::string chosen_proxy = m_cluster_table[r_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[r_cluster_id].proxy_port);
            status = m_proxy_ptrs[chosen_proxy]->deleteBlock(&context, node_block, &del_reply);
            delReplyClient->set_ifdeling(true);
            if (status.ok())
            {
                std::cout << "[DEL] deleting value of Stripe " << t_stripe_id << std::endl;
            }
        }
        catch (const std::exception &e)
        {
            std::cout << "deleteByStripe exception" << std::endl;
            std::cout << e.what() << std::endl;
        }
        return grpc::Status::OK;
    }

    grpc::Status CoordinatorImpl::listStripes(
        grpc::ServerContext *context,
        const coordinator_proto::RequestToCoordinator *req,
        coordinator_proto::RepStripeIds *listReplyClient)
    {
        try
        {
            for (auto it = m_stripe_table.begin(); it != m_stripe_table.end(); it++)
            {
                listReplyClient->add_stripe_ids(it->first);
            }
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << '\n';
        }

        return grpc::Status::OK;
    }

    grpc::Status CoordinatorImpl::checkalive(
        grpc::ServerContext *context,
        const coordinator_proto::RequestToCoordinator *helloRequestToCoordinator,
        coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator)
    {

        std::cout << "[Coordinator Check] alive " << helloRequestToCoordinator->name() << std::endl;
        return grpc::Status::OK;
    }
    grpc::Status CoordinatorImpl::reportCommitAbort(
        grpc::ServerContext *context,
        const coordinator_proto::CommitAbortStripe *commit_abortstripe,
        coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator)
    {
        ECProject::OpperateType opp = (ECProject::OpperateType)commit_abortstripe->opp();
        int stripe_id = commit_abortstripe->stripe_id();
        std::unique_lock<std::mutex> lck(m_mutex);
        try
        {
            if (commit_abortstripe->ifcommitmetadata())
            {
                if (opp == SET)
                {
                    if (IF_DEBUG)
                    {
                        std::cout << "[SET] Proxy report set stripe finish!" << std::endl;
                    }
                    Stripe &t_stripe = m_stripe_table[stripe_id];
                    for(auto it = t_stripe.object_keys.begin(); it != t_stripe.object_keys.end(); it++)
                    {
                        m_object_commit_table[(*it)] = stripe_id;
                        m_object_updating_table.erase((*it));
                    }
                    m_stripe_commit_table.push_back(stripe_id);
                    auto its = std::find(m_stripe_updating_table.begin(), m_stripe_updating_table.end(), stripe_id);
                    if (its != m_stripe_updating_table.end())
                    {
                        m_stripe_updating_table.erase(its);
                    }
                    cv.notify_all();
                }
                else if (opp == DEL) // delete the metadata
                {
                    if (IF_DEBUG)
                    {
                        std::cout << "[DEL] Proxy report delete stripe finish!" << std::endl;
                    }
                    auto its = std::find(m_stripe_updating_table.begin(), m_stripe_updating_table.end(), stripe_id);
                    if (its != m_stripe_updating_table.end())
                    {
                        m_stripe_updating_table.erase(its);
                    }
                    cv.notify_all();
                    // update stripe table
                    m_stripe_table.erase(stripe_id);
                    std::unordered_set<std::string> object_keys_set;
                    // update cluster table
                    std::map<int, Cluster>::iterator it2;
                    for (it2 = m_cluster_table.begin(); it2 != m_cluster_table.end(); it2++)
                    {
                        Cluster &t_cluster = it2->second;
                        for (auto it1 = t_cluster.blocks.begin(); it1 != t_cluster.blocks.end();)
                        {
                            if ((*it1)->map2stripe == stripe_id)
                            {
                                object_keys_set.insert((*it1)->map2key);
                                it1 = t_cluster.blocks.erase(it1);
                            }
                            else
                            {
                                it1++;
                            }
                        }
                    }
                    // update node table
                    for (auto it3 = m_node_table.begin(); it3 != m_node_table.end(); it3++)
                    {
                        Node &t_node = it3->second;
                        auto it4 = t_node.stripes.find(stripe_id);
                        if (it4 != t_node.stripes.end())
                        {
                            t_node.stripes.erase(stripe_id);
                        }
                    }
                    // update commit table
                    for (auto it5 = object_keys_set.begin(); it5 != object_keys_set.end(); it5++)
                    {
                        auto it6 = m_object_commit_table.find(*it5);
                        if (it6 != m_object_commit_table.end())
                        {
                            m_object_commit_table.erase(it6);
                        }
                    }
                }
            }
            else
            {
                Stripe &t_stripe = m_stripe_table[stripe_id];
                for(auto it = t_stripe.object_keys.begin(); it != t_stripe.object_keys.end(); it++)
                {
                    m_object_updating_table.erase(*it);
                }
                auto its = std::find(m_stripe_updating_table.begin(), m_stripe_updating_table.end(), stripe_id);
                if (its != m_stripe_updating_table.end())
                {
                    m_stripe_updating_table.erase(its);
                }
            }
        }
        catch (std::exception &e)
        {
            std::cout << "reportCommitAbort exception" << std::endl;
            std::cout << e.what() << std::endl;
        }
        return grpc::Status::OK;
    }

    grpc::Status
    CoordinatorImpl::checkCommitAbort(grpc::ServerContext *context,
                                      const coordinator_proto::AskIfSuccess *stripe_info,
                                      coordinator_proto::RepIfSuccess *reply)
    {
        std::unique_lock<std::mutex> lck(m_mutex);
        int stripe_id = stripe_info->stripe_id();
        auto it = std::find(m_stripe_updating_table.begin(), m_stripe_updating_table.end(), stripe_id);
        while (it != m_stripe_updating_table.end())
        {
            cv.wait(lck);
            it = std::find(m_stripe_updating_table.begin(), m_stripe_updating_table.end(), stripe_id);
        }
        reply->set_ifcommit(true);
        return grpc::Status::OK;
    }

    grpc::Status CoordinatorImpl::requestRepair(
        grpc::ServerContext *context,
        const coordinator_proto::FailureInfo *failures,
        coordinator_proto::RepIfRepaired *repairReplyClient)
    {
        bool isblock = failures->isblock();
        int stripe_id = -1;
        auto blocks_or_nodes = std::make_shared<std::vector<int>>() ;
        if(isblock)
        {
            stripe_id = failures->stripe_id();
        }
        for(int i = 0; i < int(failures->blocks_or_nodes_size()); i++)
        {
            blocks_or_nodes->push_back(failures->blocks_or_nodes(i));
        }

        request_repair(isblock, stripe_id, blocks_or_nodes, repairReplyClient);
        return grpc::Status::OK;
    }

} // namespace ECProject
