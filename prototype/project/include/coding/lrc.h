#ifndef LRC_H
#define LRC_H
#include "jerasure.h"
#include "reed_sol.h"
#include "cauchy.h"
#include "common.h"
#include "meta_definition.h"

namespace ECProject
{
    bool make_matrix_Azu_LRC(int k, int g, int l, int *final_matirx);
    bool make_matrix_Uniform_LRC(int k, int g, int l, int *final_matirx);
    bool make_matrix_Non_Uniform_LRC(int k, int g, int l, int *final_matirx, std::shared_ptr<std::vector<fp>> cp_ptr);
    bool encode_Azu_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size);
	bool encode_Uniform_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size);
    bool encode_Non_Uniform_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, std::shared_ptr<std::vector<fp>> cp_ptr);
    bool encode_partial_blocks_for_encoding_Azu_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, std::shared_ptr<std::vector<int>> datas_idx_ptr, std::shared_ptr<std::vector<int>> parities_idx_ptr);
    bool encode_partial_blocks_for_decoding_Azu_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, std::shared_ptr<std::vector<int>> sls_idx_ptr, std::shared_ptr<std::vector<int>> svrs_idx_ptr, std::shared_ptr<std::vector<int>> fls_idx_ptr);
    bool encode_partial_blocks_for_encoding_Uniform_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, std::shared_ptr<std::vector<int>> datas_idx_ptr, std::shared_ptr<std::vector<int>> parities_idx_ptr);
    bool encode_partial_blocks_for_decoding_Uniform_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, std::shared_ptr<std::vector<int>> sls_idx_ptr, std::shared_ptr<std::vector<int>> svrs_idx_ptr, std::shared_ptr<std::vector<int>> fls_idx_ptr);
    bool encode_partial_blocks_for_encoding_Non_Uniform_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, std::shared_ptr<std::vector<int>> datas_idx_ptr, std::shared_ptr<std::vector<int>> parities_idx_ptr, std::shared_ptr<std::vector<fp>> cp_ptr);
    bool encode_partial_blocks_for_decoding_Non_Uniform_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, std::shared_ptr<std::vector<int>> sls_idx_ptr, std::shared_ptr<std::vector<int>> svrs_idx_ptr, std::shared_ptr<std::vector<int>> fls_idx_ptr, std::shared_ptr<std::vector<fp>> cp_ptr);
    bool decode_Azu_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, int *erasures, int failed_num);
    bool decode_Uniform_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, int *erasures, int failed_num);
    bool decode_Non_Uniform_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, int block_size, int *erasures, int failed_num, std::shared_ptr<std::vector<fp>> cp_ptr);
    bool check_if_decodable_Azu_LRC(int k, int g, int l, std::shared_ptr<std::vector<int>> fls_idx_ptr);
    bool check_if_decodable_Uniform_LRC(int k, int g, int l, std::shared_ptr<std::vector<int>> fls_idx_ptr);
    bool check_if_decodable_Non_Uniform_LRC(int k, int g, int l, std::shared_ptr<std::vector<fp>> cp_ptr, std::shared_ptr<std::vector<int>> fls_idx_ptr);
    bool generate_coding_parameters_for_stripe_Azu_LRC(int &k, int &r, int &l, int g, float &x, int block_size, std::vector<int> &object_sizes);
    bool generate_coding_parameters_for_stripe_NU_LRC(int &k, int &r, int &l, int g, float &x, int block_size, std::vector<fp> &cp, std::vector<int> &object_sizes, std::vector<int> &object_accessrates);
    bool generate_coding_parameters_for_stripe_U_LRC(int &k, int &r, int &l, int g, float &x, int block_size, std::vector<int> &object_sizes);
    bool group_size_adjustment_for_stripe_NU_LRC(int g, int l, int real_l, int block_size, std::vector<fp> &cp);
    bool predict_repair_cost_flat(int k, int r, int g, std::vector<fp> &cp, std::vector<int> &object_accessrates);
    bool predict_repair_cost_hierachical(int k, int r, int g, std::vector<fp> &cp, std::vector<int> &object_accessrates);
    bool generate_stripe_information_for_NU_LRC(int k, int g, std::vector<std::vector<int>> &stripe_info, std::map<int, int> &b2g, std::vector<fp> &cp);
    bool generate_stripe_information_for_U_LRC(int k, int r, int g, std::vector<std::vector<int>> &stripe_info, std::map<int, int> &b2g);
    bool generate_stripe_information_for_Azu_LRC(int k, int r, int g, std::vector<std::vector<int>> &stripe_info, std::map<int, int> &b2g);
    
}

#endif