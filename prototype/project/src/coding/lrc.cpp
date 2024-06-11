#include <lrc.h>
#include <common.h>

template <typename T>
inline T ceil(T const &A, T const &B)
{
  return T((A + B - 1) / B);
};

bool ECProject::make_matrix_Azu_LRC(int k, int g, int l, int *final_matrix)
{
	int r = (k + l - 1) / l;
    int *matrix = reed_sol_vandermonde_coding_matrix(k, g, 8);
    
    bzero(final_matrix, sizeof(int) * k * (g + l));

    for(int i = 0; i < g; i++)
    {
        for(int j = 0; j < k; j++)
        {
            final_matrix[i * k + j] = matrix[i * k + j];
        }
    }

    for(int i = 0; i < l; i++)
    {
        for(int j = 0; j < k; j++)
        {
            if(i * r <= j && j < (i + 1) * r)
            {
                final_matrix[(i + g) * k + j] = 1;
            }
        }
    }

    free(matrix);
    return true;
}

bool ECProject::make_matrix_Uniform_LRC(int k, int g, int l, int *final_matrix)
{
    int r = (k + g + l - 1) / l;
    // int *matrix = reed_sol_vandermonde_coding_matrix(k, g, 8);
    int *matrix = cauchy_good_general_coding_matrix(k, g+1, 8);

    bzero(final_matrix, sizeof(int) * k * (g + l));

    for (int i = 0; i < g; i++)
    {
        for (int j = 0; j < k; j++)
        {
            final_matrix[i * k + j] = matrix[i * k + j];
        }
    }

    std::vector<int> l_matrix(l * (k + g), 0);
    std::vector<int> d_g_matrix((k + g) * k, 0);
    int idx = 0;
    for (int i = 0; i < l; i++)
    {
        int group_size = std::min(r, k + g - i * r);
        for (int j = 0; j < group_size; j++)
        {
            l_matrix[i * (k + g) + idx] = 1;
            idx++;
        }
    }
    for (int i = 0; i < k; i++)
    {
        d_g_matrix[i * k + i] = 1;
    }
    idx = k * k;
    for (int i = 0; i < g; i++)
    {
        for (int j = 0; j < k; j++)
        {
            d_g_matrix[idx + i * k + j] = matrix[i * k + j];
        }
    }

    // print_matrix(l_matrix.data(), l, k + g);
    // print_matrix(d_g_matrix.data(), k + g, k);

    int *mix_matrix = jerasure_matrix_multiply(l_matrix.data(), d_g_matrix.data(), l, k + g, k + g, k, 8);

    idx = g * k;
    for (int i = 0; i < l; i++)
    {
        for (int j = 0; j < k; j++)
        {
            final_matrix[idx + i * k + j] = mix_matrix[i * k + j];
        }
    }

    free(matrix);
    free(mix_matrix);
    return true;
}

bool ECProject::make_matrix_Non_Uniform_LRC(int k, int g, int l, int *final_matrix, std::shared_ptr<std::vector<fp>> cp_ptr)
{
    // int *matrix = reed_sol_vandermonde_coding_matrix(k, g, 8);
    int *matrix = cauchy_good_general_coding_matrix(k, g+1, 8);

    bzero(final_matrix, sizeof(int) * k * (g + l));

    for (int i = 0; i < g; i++)
    {
        for (int j = 0; j < k; j++)
        {
            final_matrix[i * k + j] = matrix[i * k + j];
        }
    }

    std::vector<int> l_matrix(l * (k + g), 0);
    std::vector<int> d_g_matrix((k + g) * k, 0);
    int n = int(cp_ptr->size()) - 1;
    int cnt = 0, b_idx = 0, l_idx = 0;
    std::vector<int> vfb;
    for (auto it = cp_ptr->begin(); it != cp_ptr->end(); it++, cnt++)
    {
        fp tmp = (*it);
        int ki = int(tmp.ki), ri = int(tmp.ri);
        if (cnt < n)
        {
            if (ri > 0)
            {
                for (int li = 0; li < ki / ri; li++)
                {
                    for (int j = 0; j < ri; j++)
                    {
                        l_matrix[l_idx * (k + g) + b_idx] = 1;
                        b_idx++;
                    }
                    l_idx++;
                }
                for (int j = 0; j < ki % ri; j++)
                {
                    vfb.push_back(b_idx++);
                }
            }
            else
            {
                for (int j = 0; j < ki; j++)
                {
                    vfb.push_back(b_idx++);
                }
            }
        }
        else
        {
            for (int j = 0; j < g; j++)
            {
                vfb.push_back(b_idx++);
            }
            if (ki != int(vfb.size()))
            {
                std::cout << "Coding Error! Length not match!" << std::endl;
            }
            int vfb_idx = 0;
            for (int li = 0; li < (ki + ri - 1) / ri; li++)
            {
                int group_size = std::min(ri, ki - li * ri);
                for (int j = 0; j < group_size; j++)
                {
                    b_idx = vfb[vfb_idx++];
                    l_matrix[l_idx * (k + g) + b_idx] = 1;
                }
                l_idx++;
            }
        }
    }

    for (int i = 0; i < k; i++)
    {
        d_g_matrix[i * k + i] = 1;
    }
    int idx = k * k;
    for (int i = 0; i < g; i++)
    {
        for (int j = 0; j < k; j++)
        {
            d_g_matrix[idx + i * k + j] = matrix[i * k + j];
        }
    }
    // std::cout << "l_matrix: " << std::endl;
    // print_matrix(l_matrix.data(), l, k + g);
    // std::cout << "d_g_matrix: " << std::endl;
    // print_matrix(d_g_matrix.data(), k + g, k);
    int *mix_matrix = jerasure_matrix_multiply(l_matrix.data(), d_g_matrix.data(), l, k + g, k + g, k, 8);

    idx = g * k;
    for (int i = 0; i < l; i++)
    {
        for (int j = 0; j < k; j++)
        {
            final_matrix[idx + i * k + j] = mix_matrix[i * k + j];
        }
    }

    free(matrix);
    free(mix_matrix);
    return true;
}

bool ECProject::encode_Azu_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size)
{
	std::vector<int> lrc_matrix((g + l) * k, 0);
	make_matrix_Azu_LRC(k, g, l, lrc_matrix.data());
	jerasure_matrix_encode(k, g + l, 8, lrc_matrix.data(), data_ptrs, coding_ptrs, block_size);
    return true;
}

bool ECProject::encode_Uniform_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size)
{
    std::vector<int> lrc_matrix((g + l) * k, 0);
    make_matrix_Uniform_LRC(k, g, l, lrc_matrix.data());
    jerasure_matrix_encode(k, g + l, 8, lrc_matrix.data(), data_ptrs, coding_ptrs, block_size);
    return true;
}

bool ECProject::encode_Non_Uniform_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, std::shared_ptr<std::vector<fp>> cp_ptr)
{
    std::vector<int> lrc_matrix((g + l) * k, 0);
    make_matrix_Non_Uniform_LRC(k, g, l, lrc_matrix.data(), cp_ptr);
    jerasure_matrix_encode(k, g + l, 8, lrc_matrix.data(), data_ptrs, coding_ptrs, block_size);
    return true;
}

bool ECProject::encode_partial_blocks_for_encoding_Azu_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, std::shared_ptr<std::vector<int>> datas_idx_ptr, std::shared_ptr<std::vector<int>> parities_idx_ptr)
{
	std::vector<int> lrc_matrix((k + g + l) * k, 0);
    get_full_matrix(lrc_matrix.data(), k);
	make_matrix_Azu_LRC(k, g, l, &(lrc_matrix.data())[k * k]);
	return encode_partial_blocks_for_encoding(k, g + l, lrc_matrix.data(), data_ptrs, coding_ptrs, block_size, datas_idx_ptr, parities_idx_ptr);
}

bool ECProject::encode_partial_blocks_for_decoding_Azu_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, std::shared_ptr<std::vector<int>> sls_idx_ptr, std::shared_ptr<std::vector<int>> svrs_idx_ptr, std::shared_ptr<std::vector<int>> fls_idx_ptr)
{
	std::vector<int> lrc_matrix((k + g + l) * k, 0);
    get_full_matrix(lrc_matrix.data(), k);
    make_matrix_Azu_LRC(k, g, l, &(lrc_matrix.data())[k * k]);
	return encode_partial_blocks_for_decoding(k, g + l, lrc_matrix.data(), data_ptrs, coding_ptrs, block_size, sls_idx_ptr, svrs_idx_ptr, fls_idx_ptr);
}

bool ECProject::encode_partial_blocks_for_encoding_Uniform_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, std::shared_ptr<std::vector<int>> datas_idx_ptr, std::shared_ptr<std::vector<int>> parities_idx_ptr)
{
    std::vector<int> lrc_matrix((k + g + l) * k, 0);
    get_full_matrix(lrc_matrix.data(), k);
    make_matrix_Uniform_LRC(k, g, l, &(lrc_matrix.data())[k * k]);
    return encode_partial_blocks_for_encoding(k, g + l, lrc_matrix.data(), data_ptrs, coding_ptrs, block_size, datas_idx_ptr, parities_idx_ptr);
}

bool ECProject::encode_partial_blocks_for_decoding_Uniform_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, std::shared_ptr<std::vector<int>> sls_idx_ptr, std::shared_ptr<std::vector<int>> svrs_idx_ptr, std::shared_ptr<std::vector<int>> fls_idx_ptr)
{
    std::vector<int> lrc_matrix((k + g + l) * k, 0);
    get_full_matrix(lrc_matrix.data(), k);
    make_matrix_Uniform_LRC(k, g, l, &(lrc_matrix.data())[k * k]);
    return encode_partial_blocks_for_decoding(k, g + l, lrc_matrix.data(), data_ptrs, coding_ptrs, block_size, sls_idx_ptr, svrs_idx_ptr, fls_idx_ptr);
}

bool ECProject::encode_partial_blocks_for_encoding_Non_Uniform_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, std::shared_ptr<std::vector<int>> datas_idx_ptr, std::shared_ptr<std::vector<int>> parities_idx_ptr, std::shared_ptr<std::vector<fp>> cp_ptr)
{
    std::vector<int> lrc_matrix((k + g + l) * k, 0);
    get_full_matrix(lrc_matrix.data(), k);
    make_matrix_Non_Uniform_LRC(k, g, l, &(lrc_matrix.data())[k * k], cp_ptr);
    return encode_partial_blocks_for_encoding(k, g + l, lrc_matrix.data(), data_ptrs, coding_ptrs, block_size, datas_idx_ptr, parities_idx_ptr);
}

bool ECProject::encode_partial_blocks_for_decoding_Non_Uniform_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, std::shared_ptr<std::vector<int>> sls_idx_ptr, std::shared_ptr<std::vector<int>> svrs_idx_ptr, std::shared_ptr<std::vector<int>> fls_idx_ptr, std::shared_ptr<std::vector<fp>> cp_ptr)
{
    std::vector<int> lrc_matrix((k + g + l) * k, 0);
    get_full_matrix(lrc_matrix.data(), k);
    make_matrix_Non_Uniform_LRC(k, g, l, &(lrc_matrix.data())[k * k], cp_ptr);
    return encode_partial_blocks_for_decoding(k, g + l, lrc_matrix.data(), data_ptrs, coding_ptrs, block_size, sls_idx_ptr, svrs_idx_ptr, fls_idx_ptr);
}

bool ECProject::decode_Azu_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, int *erasures, int failed_num)
{
    std::vector<int> lrc_matrix((g + l) * k, 0);
    make_matrix_Azu_LRC(k, g, l, lrc_matrix.data());
    int i = 0;
    i = jerasure_matrix_decode(k, g + l, 8, lrc_matrix.data(), failed_num, erasures, data_ptrs, coding_ptrs, block_size);
    if(i == -1)
    {
        std::cout << "[Decode] Failed!" << std::endl;
        return false;
    }
    return true;
}    

bool ECProject::decode_Uniform_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, int *erasures, int failed_num)
{
    std::vector<int> lrc_matrix((g + l) * k, 0);
    make_matrix_Uniform_LRC(k, g, l, lrc_matrix.data());
    int i = 0;
    i = jerasure_matrix_decode(k, g + l, 8, lrc_matrix.data(), failed_num, erasures, data_ptrs, coding_ptrs, block_size);
    if (i == -1)
    {
        std::cout << "[Decode] Failed!" << std::endl;
        return false;
    }
    return true;
}

bool ECProject::decode_Non_Uniform_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, int *erasures, int failed_num, std::shared_ptr<std::vector<fp>> cp_ptr)
{
    std::vector<int> lrc_matrix((g + l) * k, 0);
    make_matrix_Non_Uniform_LRC(k, g, l, lrc_matrix.data(), cp_ptr);
    int i = 0;
    i = jerasure_matrix_decode(k, g + l, 8, lrc_matrix.data(), failed_num, erasures, data_ptrs, coding_ptrs, block_size);
    if (i == -1)
    {
        std::cout << "[Decode] Failed!" << std::endl;
        return false;
    }
    return true;
}

bool ECProject::check_if_decodable_Azu_LRC(int k, int g, int l, std::shared_ptr<std::vector<int>> fls_idx_ptr)
{
    int r = (k + l - 1) / l;
    std::map<int, int> b2g;
    std::vector<int> group_fd_cnt;
    std::vector<int> group_fgp_cnt;
    std::vector<int> group_slp_cnt;
    int sgp_cnt = g;
    int idx = 0;
    for (int i = 0; i < l; i++)
    {
        int group_size = std::min(r, k - i * r);
        for (int j = 0; j < group_size; j++)
        {
            b2g.insert(std::make_pair(idx, i));
            idx++;
        }
        b2g.insert(std::make_pair(k + g + i, i));
        group_fd_cnt.push_back(0);
        group_slp_cnt.push_back(1);
    }

    for (auto it = fls_idx_ptr->begin(); it != fls_idx_ptr->end(); it++)
    {
        int block_id = *it;
        if (block_id < k)
        {
            group_fd_cnt[b2g[block_id]] += 1;
        }
        else if (block_id < k + g && block_id >= k)
        {
            sgp_cnt -= 1;
        }
        else
        {
            group_slp_cnt[block_id - k - g] -= 1;
        }
    }
    for (int i = 0; i < l; i++)
    {
        if (group_slp_cnt[i] && group_slp_cnt[i] <= group_fd_cnt[i])
        {
            group_fd_cnt[i] -= group_slp_cnt[i];
            group_slp_cnt[i] = 0;
        }
    }
    for (int i = 0; i < l; i++)
    {
        if (sgp_cnt >= group_fd_cnt[i])
        {
            sgp_cnt -= group_fd_cnt[i];
            group_fd_cnt[i] = 0;
        }
        else
        {
            return false;
        }
    }
    return true;
}

bool ECProject::check_if_decodable_Uniform_LRC(int k, int g, int l, std::shared_ptr<std::vector<int>> fls_idx_ptr)
{
    int r = (k + g + l - 1) / l;
    std::map<int, int> b2g;
    std::vector<int> group_fd_cnt;
    std::vector<int> group_fgp_cnt;
    std::vector<int> group_slp_cnt;
    std::vector<bool> group_pure_flag;
    int sgp_cnt = g;
    int idx = 0;
    for (int i = 0; i < l; i++)
    {
        int group_size = std::min(r, k + g - i * r);
        for (int j = 0; j < group_size; j++)
        {
            b2g.insert(std::make_pair(idx, i));
            idx++;
        }
        if (idx <= k || idx - group_size >= k)
        {
            group_pure_flag.push_back(true);
        }
        else
        {
            group_pure_flag.push_back(false);
        }
        b2g.insert(std::make_pair(k + g + i, i));
        group_fd_cnt.push_back(0);
        group_fgp_cnt.push_back(0);
        group_slp_cnt.push_back(1);
    }

    for (auto it = fls_idx_ptr->begin(); it != fls_idx_ptr->end(); it++)
    {
        int block_id = *it;
        if (block_id < k)
        {
            group_fd_cnt[b2g[block_id]] += 1;
        }
        else if (block_id < k + g && block_id >= k)
        {
            group_fgp_cnt[b2g[block_id]] += 1;
            sgp_cnt -= 1;
        }
        else
        {
            group_slp_cnt[block_id - k - g] -= 1;
        }
    }

    for (int i = 0; i < l; i++)
    {
        if (group_slp_cnt[i] && group_pure_flag[i])
        {
            if (group_slp_cnt[i] <= group_fd_cnt[i])
            {
                group_fd_cnt[i] -= group_slp_cnt[i];
                group_slp_cnt[i] = 0;
            }
            if (group_slp_cnt[i] && group_slp_cnt[i] == group_fgp_cnt[i])
            {
                group_fgp_cnt[i] -= group_slp_cnt[i];
                group_slp_cnt[i] = 0;
                sgp_cnt += 1;
            }
        }
        else if (group_slp_cnt[i] && !group_pure_flag[i])
        {
            if (group_fd_cnt[i] == 1 && !group_fgp_cnt[i])
            {
                group_fd_cnt[i] -= group_slp_cnt[i];
                group_slp_cnt[i] = 0;
            }
            else if (group_fgp_cnt[i] == 1 && !group_fd_cnt[i])
            {
                group_fgp_cnt[i] -= group_slp_cnt[i];
                group_slp_cnt[i] = 0;
                sgp_cnt += 1;
            }
        }
    }
    for (int i = 0; i < l; i++)
    {
        if (sgp_cnt >= group_fd_cnt[i])
        {
            sgp_cnt -= group_fd_cnt[i];
            group_fd_cnt[i] = 0;
        }
        else
        {
            return false;
        }
    }
    return true;
}

bool ECProject::check_if_decodable_Non_Uniform_LRC(int k, int g, int l, std::shared_ptr<std::vector<fp>> cp_ptr, std::shared_ptr<std::vector<int>> fls_idx_ptr)
{
    // int r = (k + g + l - 1) / l;
    std::map<int, int> b2g;
    std::vector<int> group_fd_cnt;
    std::vector<int> group_fgp_cnt;
    std::vector<int> group_slp_cnt;
    std::vector<bool> group_pure_flag;
    int sgp_cnt = g;
    
    int n = int(cp_ptr->size()) - 1;
    int cnt = 0, b_idx = 0, l_idx = 0;
    std::vector<int> vfb;
    for (auto it = cp_ptr->begin(); it != cp_ptr->end(); it++, cnt++)
    {
        fp tmp = (*it);
        int ki = int(tmp.ki), ri = int(tmp.ri);
        if (cnt < n)
        {
            if (ri > 0)
            {
                for (int li = 0; li < ki / ri; li++)
                {
                    for (int j = 0; j < ri; j++)
                    {
                        b2g.insert(std::make_pair(b_idx, l_idx));
                        b_idx++;
                    }
                    if(b_idx <= k || b_idx - ri >= k)
                    {
                        group_pure_flag.push_back(true);
                    }
                    else
                    {
                        group_pure_flag.push_back(false);
                    }
                    b2g.insert(std::make_pair(k + g + l_idx, l_idx));
                    group_fd_cnt.push_back(0);
                    group_fgp_cnt.push_back(0);
                    group_slp_cnt.push_back(1);
                    l_idx++;
                }
                for (int j = 0; j < ki % ri; j++)
                {
                    vfb.push_back(b_idx++);
                }
            }
            else
            {
                for (int j = 0; j < ki; j++)
                {
                    vfb.push_back(b_idx++);
                }
            }
        }
        else
        {
            for (int j = 0; j < g; j++)
            {
                vfb.push_back(b_idx++);
            }
            if (ki != int(vfb.size()))
            {
                std::cout << "Coding Error! Length not match!" << std::endl;
            }
            int vfb_idx = 0;
            for (int li = 0; li < (ki + ri - 1) / ri; li++)
            {
                int group_size = std::min(ri, ki - li * ri);
                for (int j = 0; j < group_size; j++)
                {
                    b_idx = vfb[vfb_idx++];
                    b2g.insert(std::make_pair(b_idx, l_idx));
                }
                if(b_idx < k || b_idx - group_size > k)
                {
                    group_pure_flag.push_back(true);
                }
                else
                {
                    group_pure_flag.push_back(false);
                }
                b2g.insert(std::make_pair(k + g + l_idx, l_idx));
                group_fd_cnt.push_back(0);
                group_fgp_cnt.push_back(0);
                group_slp_cnt.push_back(1);
                l_idx++;
            }
        }
    }

    for (auto it = fls_idx_ptr->begin(); it != fls_idx_ptr->end(); it++)
    {
        int block_id = *it;
        if (block_id < k)
        {
            group_fd_cnt[b2g[block_id]] += 1;
        }
        else if (block_id < k + g && block_id >= k)
        {
            group_fgp_cnt[b2g[block_id]] += 1;
            sgp_cnt -= 1;
        }
        else
        {
            group_slp_cnt[block_id - k - g] -= 1;
        }
    }

    for (int i = 0; i < l; i++)
    {
        // std::cout << i << " " << group_fgp_cnt[i] << " " << group_pure_flag[i] << std::endl;
        if (group_slp_cnt[i] && group_pure_flag[i])
        {
            if (group_slp_cnt[i] <= group_fd_cnt[i])
            {
                group_fd_cnt[i] -= group_slp_cnt[i];
                group_slp_cnt[i] = 0;
            }
            if (group_slp_cnt[i] && group_slp_cnt[i] == group_fgp_cnt[i])
            {
                group_fgp_cnt[i] -= group_slp_cnt[i];
                group_slp_cnt[i] = 0;
                sgp_cnt += 1;
            }
        }
        else if (group_slp_cnt[i] && !group_pure_flag[i])
        {
            if (group_fd_cnt[i] == 1 && !group_fgp_cnt[i])
            {
                group_fd_cnt[i] -= group_slp_cnt[i];
                group_slp_cnt[i] = 0;
            }
            else if (group_fgp_cnt[i] == 1 && !group_fd_cnt[i])
            {
                group_fgp_cnt[i] -= group_slp_cnt[i];
                group_slp_cnt[i] = 0;
                sgp_cnt += 1;
            }
        }
    }
    for (int i = 0; i < l; i++)
    {
        // std::cout << i << " " << group_fd_cnt[i] << " " << sgp_cnt << std::endl;
        if (sgp_cnt >= group_fd_cnt[i])
        {
            sgp_cnt -= group_fd_cnt[i];
            group_fd_cnt[i] = 0;
        }
        else
        {
            return false;
        }
    }
    return true;
}

bool ECProject::generate_coding_parameters_for_stripe_Azu_LRC(int &k, int &r, int &l, int g, float &x, int block_size, std::vector<int> &object_sizes)
{
    k = 0;
    for(auto it = object_sizes.begin(); it != object_sizes.end(); it++)
    {
        int tmp = (*it) / block_size;
        k += tmp;
    }
    l = int(round(x * float(k))) - k - g;
    r = (k + l - 1) / l;
    if((k + r - 1) / r != l)
    {
        l = (k + r - 1) / r;
        x = float(k + g + l) / float(k);
    }

    // std::cout << "Azure-LRC(" << k << "," << r << "," << g << ")\n";

    return true;
}

bool ECProject::generate_coding_parameters_for_stripe_NU_LRC(int &k, int &r, int &l, int g, float &x, int block_size, std::vector<fp> &cp, std::vector<int> &object_sizes, std::vector<int> &object_accessrates)
{
        int n = int(object_sizes.size());
        k = 0;
        std::vector<int> object_length;
        for(auto it = object_sizes.begin(); it != object_sizes.end(); it++)
        {
            int tmp = (*it) / block_size;
            object_length.push_back(tmp);
            k += tmp;
        }
        l = int(round(x * float(k))) - k - g;
        // float k_avg = float(k) / float(n);
        r = (k + g + l - 1) / l;
        if((k + g + r - 1) / r != l)
        {
            l = (k + g + r - 1) / r;
            x = float(k + g + l) / float(k);
        }

        int ar_w = 0;
        for(int i = 0; i < n; i++)
        {
            ar_w += object_accessrates[i] * object_length[i];
        }

        float ar_w_avg = float(ar_w) / float(k);

        std::vector<int> group_sizes;
        int kn = 0, l_sum = 0;
        for(int i = 0; i < n; i++)
        {
            // int gsi = std::max(int(round((float(r) * ar_w_avg * k_avg) / (float(object_accessrates[i] * object_length[i])))), 1);
            int gsi = std::max(int(ceil(float(object_length[i] * (ar_w + ar_w_avg * g)), float(object_accessrates[i] * object_length[i] * l))), 1);
            if(gsi > object_length[i])
            {
                gsi = 0;
                kn += object_length[i];
            }
            group_sizes.push_back(gsi);
            if(gsi > 0)
            {
                l_sum += object_length[i] / gsi;
                kn += object_length[i] % gsi;
            }
        }

        kn += g;
        int last_group_size = std::min(int(ceil(kn, std::max(l - l_sum, 1))), kn);
        l_sum += ceil(kn, last_group_size);

        cp.clear();
        for(int i = 0; i < n; i++)
        {
            fp tmp;
            tmp.ki = (ushort)object_length[i];
            tmp.ri = (ushort)group_sizes[i];
            cp.push_back(tmp);
        }

        fp tmp;
        tmp.ki = (ushort)kn;
        tmp.ri = (ushort)last_group_size;
        cp.push_back(tmp);

        // std::cout << "Before adjustment: Non-uniform LRC(";
        // for(int i = 0; i < n + 1; i++)
        // {
        //     std::cout << "(" << cp[i].ki << "," << cp[i].ri << "),";
        // }
        // std::cout << g << ")\n";

        // adjust
        if(l_sum > l)
        {
            group_size_adjustment_for_stripe_NU_LRC(g, l, l_sum, block_size, cp);
        }        

        // std::cout << "After adjustment: Non-uniform LRC(";
        // for(int i = 0; i < n + 1; i++)
        // {
        //     std::cout << "(" << cp[i].ki << "," << cp[i].ri << "),";
        // }
        // std::cout << g << ")\n";

        return true;
}

bool ECProject::group_size_adjustment_for_stripe_NU_LRC(int g, int l, int real_l, int block_size, std::vector<fp> &cp)
{
    int n_1 = int(cp.size());
    int new_l = 0;
    std::vector<int> new_group_size;
    std::vector<int> new_local_nums;
    int new_kn = 0;
    for(int i = 0; i < n_1 - 1; i++)
    {
        int new_ri = 0;
        int old_ri = int(cp[i].ri);
        int ki = int(cp[i].ki);
        if(old_ri > 0)
        {
            new_ri = std::min(int(round(float(old_ri * real_l)) / float(l)), ki);
            new_kn += ki % old_ri;
            int new_li = ki / new_ri;
            new_l += new_li;
            new_local_nums.push_back(new_li);
        }
        else
        {
            new_kn += ki;
        }
        new_group_size.push_back(new_ri);
    }
    new_kn += g;
    int new_rn = std::min(ceil(new_kn, std::max(l - new_l, 1)), new_kn);
    int new_ln = ceil(new_kn, new_rn);
    new_l += new_ln;
    new_group_size.push_back(new_rn);
    new_local_nums.push_back(new_ln);
    cp[n_1 - 1].ki = ushort(new_kn);
    if(new_l == l)
    {
        for(int i = 0; i < n_1; i++)
        {
            cp[i].ri = ushort(new_group_size[i]);
        }
    }
    else
    {
        new_kn = 0;
        for(int i = 0; i < n_1 - 1; i++)
        {
            cp[i].ri = ushort(0);
            new_kn += int(cp[i].ki);
        }
        new_kn += g;
        new_rn = std::min(ceil(new_kn, std::max(l, 1)), new_kn);
        cp[n_1 - 1].ki = ushort(new_kn);
        cp[n_1 - 1].ri = ushort(new_rn);
    }
    return true;
}

bool ECProject::generate_coding_parameters_for_stripe_U_LRC(int &k, int &r, int &l, int g, float &x, int block_size, std::vector<int> &object_sizes)
{
        // int n = int(object_sizes.size());
        k = 0;
        for(auto it = object_sizes.begin(); it != object_sizes.end(); it++)
        {
            int tmp = (*it) / block_size;
            k += tmp;
        }
        l = int(round(x * float(k))) - k - g;
        r = (k + g + l - 1) / l;
        if((k + g + r - 1) / r != l)
        {
            l = (k + g + r - 1) / r;
            x = float(k + g + l) / float(k);
        }

        // std::cout << "Uniform LRC(" << k << "," << r << "," << g << ")\n";

        return true;
}

bool ECProject::predict_repair_cost_flat(int k, int r, int g, std::vector<fp> &cp, std::vector<int> &object_accessrates)
{
        int n = int(object_accessrates.size());
        std::vector<float> w;
        int a_sum = 0;
        for(int i = 0; i < n; i++)
        {
            a_sum += object_accessrates[i];
        }
        float w_avg = float(a_sum) / float(n);
        for(int i = 0; i < n; i++)
        {
            w.push_back(float(object_accessrates[i]) / w_avg);
        }

        // Uniform LRC
        float cost_u = 0.0;
        int last_group_size = (k + g) % r;
        int bias = 0;
        if(last_group_size > g)
        {
            bias = last_group_size - g;
        }
        int block_idx = 0;
        for(int i = 0; i < n; i++)
        {
            for(int j = 0; j < int(cp[i].ki); j++)
            {
                int group_size = r;
                if(block_idx >= k - bias)
                {
                    group_size = last_group_size;
                }
                cost_u += float(group_size) * w[i];
                block_idx++;
            }
        }
        cost_u /= k;

        // Non-uniform LRC
        float cost_nu = 0.0;
        int k_remain = 0;
        std::vector<int> idx;
        for(int i = 0; i < n; i++)
        {
            int remain = (int)cp[i].ki;
            if(cp[i].ri)
            {
                remain = int(cp[i].ki % cp[i].ri);
            }
            k_remain += remain;
            for(int j = 0; j < remain; j++)
            {
                idx.push_back(i);
            }
        }
        int kn = k_remain + g;
        int rn = std::min(kn, int(cp[n].ri));
        int kn_rn = kn % rn;
        bias = 0;
        if(kn_rn > g)
        {
            bias = kn_rn - g;
        }
        for(int i = 0; i < k_remain; i++)
        {
            if(i < kn - bias)
            {
                cost_nu += float(rn) * w[idx[i]];
            }
            else
            {
                cost_nu += float(kn_rn) * w[idx[i]];
            }
        }
        for(int i = 0; i < n; i++)
        {
            int num = (int)cp[i].ki;
            if(cp[i].ri)
            {
                num = int(cp[i].ki - (cp[i].ki % cp[i].ri));
            }
            cost_nu += float(num) * float(cp[i].ri) * w[i];
        }
        cost_nu /= k;

        std::cout << "cost_u:" << cost_u << ", cost_nu:" << cost_nu << std::endl;

        if(cost_nu > cost_u)
            return false;
        return true;
}

// applying encode-and-transfer
bool ECProject::predict_repair_cost_hierachical(int k, int r, int g, std::vector<fp> &cp, std::vector<int> &object_accessrates)
{
        int n = int(object_accessrates.size());
        std::vector<float> w;
        int a_sum = 0;
        for(int i = 0; i < n; i++)
        {
            a_sum += object_accessrates[i];
        }
        float w_avg = float(a_sum) / float(n);
        for(int i = 0; i < n; i++)
        {
            w.push_back(float(object_accessrates[i]) / w_avg);
        }

        // Uniform LRC
        float cost_u = 0.0;
        int last_group_size = (k + g) % r;
        int bias = 0;
        if(last_group_size > g)
        {
            bias = last_group_size - g;
        }
        int block_idx = 0;
        for(int i = 0; i < n; i++)
        {
            for(int j = 0; j < int(cp[i].ki); j++)
            {
                int group_size = r;
                if(block_idx >= k - bias)
                {
                    group_size = last_group_size;
                }
                int c = (group_size + 1 + g) / (g + 1) - 1;
                cost_u += float(c) * w[i];
                block_idx++;
            }
        }
        cost_u /= k;

        // Non-uniform LRC
        float cost_nu = 0.0;
        int k_remain = 0;
        std::vector<int> idx;
        for(int i = 0; i < n; i++)
        {
            int remain = (int)cp[i].ki;
            if(cp[i].ri)
            {
                remain = int(cp[i].ki % cp[i].ri);
            }
            k_remain += remain;
            for(int j = 0; j < remain; j++)
            {
                idx.push_back(i);
            }
        }
        int kn = k_remain + g;
        int rn = std::min(kn, int(cp[n].ri));
        int kn_rn = kn % rn;
        bias = 0;
        if(kn_rn > g)
        {
            bias = kn_rn - g;
        }
        for(int i = 0; i < k_remain; i++)
        {
            if(i < kn - bias)
            {
                cost_nu += float((rn + 1 + g) / (g + 1) - 1) * w[idx[i]];
            }
            else
            {
                cost_nu += float((kn_rn + 1 + g) / (g + 1) - 1) * w[idx[i]];
            }
        }
        for(int i = 0; i < n; i++)
        {
            int num = (int)cp[i].ki;
            if(cp[i].ri)
            {
                num = int(cp[i].ki - (cp[i].ki % cp[i].ri));
            }
            cost_nu += float(num) * float((cp[i].ri + 1 + g) / (g + 1) - 1) * w[i];
        }
        cost_nu /= k;

        std::cout << "cost_u:" << cost_u << ", cost_nu:" << cost_nu << std::endl;

        if(cost_nu > cost_u)
            return false;
        return true;
}

bool ECProject::generate_stripe_information_for_NU_LRC(int k, int g, std::vector<std::vector<int>> &stripe_info, std::map<int, int> &b2g, std::vector<fp> &cp)
{
        int n = int(cp.size()) - 1;
        int cnt = 0, b_idx = 0, l_idx = 0;
        std::vector<int> vfb;
        for (auto it = cp.begin(); it != cp.end(); it++, cnt++)
        {
            fp tmp = (*it);
            int ki = int(tmp.ki), ri = int(tmp.ri);
            if (cnt < n)
            {
                if (ri > 0)
                {
                    for (int li = 0; li < ki / ri; li++)
                    {
                        std::vector<int> local_group;
                        for (int j = 0; j < ri; j++)
                        {
                            local_group.push_back(b_idx);
                            b2g[b_idx] = l_idx;
                            b_idx++;
                        }
                        b2g[k + g + l_idx] = l_idx;
                        local_group.push_back(k + g + l_idx);
                        stripe_info.push_back(local_group);
                        l_idx++;
                    }
                    for (int j = 0; j < ki % ri; j++)
                    {
                        vfb.push_back(b_idx++);
                    }
                }
                else
                {
                    for (int j = 0; j < ki; j++)
                    {
                        vfb.push_back(b_idx++);
                    }
                }
            }
            else
            {
                for (int j = 0; j < g; j++)
                {
                    vfb.push_back(b_idx++);
                }
                if (ki != int(vfb.size()))
                {
                    std::cout << "Coding Error! Length not match!" << std::endl;
                }
                int vfb_idx = 0;
                for (int li = 0; li < (ki + ri - 1) / ri; li++)
                {
                    std::vector<int> local_group;
                    int group_size = std::min(ri, ki - li * ri);
                    for (int j = 0; j < group_size; j++)
                    {
                        b_idx = vfb[vfb_idx++];
                        local_group.push_back(b_idx);
                        b2g[b_idx] = l_idx;
                    }
                    b2g[k + g + l_idx] = l_idx;
                    local_group.push_back(k + g + l_idx);
                    stripe_info.push_back(local_group);
                    l_idx++;
                }
            }
        }
        return true;
}

bool ECProject::generate_stripe_information_for_U_LRC(int k, int r, int g, std::vector<std::vector<int>> &stripe_info, std::map<int, int> &b2g)
{
        int l = (k + g + r - 1) / r;
        int idx = 0;
        for(int i = 0; i < l; i++)
        {
            std::vector<int> local_group;
            int group_size = std::min(r, k + g - i * r);
            for (int j = 0; j < group_size; j++)
            {
                local_group.push_back(idx);
                b2g[idx] = i;
                idx++;
            }
            b2g[k + g + i] = i;
            local_group.push_back(k + g + i);
            stripe_info.push_back(local_group);
        }
        return true;
}

bool ECProject::generate_stripe_information_for_Azu_LRC(int k, int r, int g, std::vector<std::vector<int>> &stripe_info, std::map<int, int> &b2g)
{
        int l = (k + r - 1) / r;
        int idx = 0;
        for(int i = 0; i < l; i++)
        {
            std::vector<int> local_group;
            int group_size = std::min(r, k - i * r);
            for (int j = 0; j < group_size; j++)
            {
                local_group.push_back(idx);
                b2g[idx] = i;
                idx++;
            }
            b2g[k + g + i] = i;
            local_group.push_back(k + g + i);
            stripe_info.push_back(local_group);
        }
        std::vector<int> local_group;
        for(int i = 0; i < g; i++)
        {
            local_group.push_back(idx);
            b2g[idx] = l;
            idx++;
        }
        stripe_info.push_back(local_group);
        return true;
}