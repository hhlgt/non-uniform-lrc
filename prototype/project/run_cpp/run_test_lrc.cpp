#include <lrc.h>
#include <meta_definition.h>
#include <set>
#include <vector>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <sys/stat.h>

void test_decode(int k, int g, int l, int block_size, std::string targetdir, std::shared_ptr<std::vector<ECProject::fp>> cp_ptr, ECProject::EncodeType encode_type);
void test_partail_decode_global(int k, int g, int l, int block_size, std::string targetdir, std::shared_ptr<std::vector<ECProject::fp>> cp_ptr, ECProject::EncodeType encode_type);
void test_partail_decode_local(int k, int g, int l, int block_size, std::string targetdir);
void test_decodability(int k, int g, int l, std::shared_ptr<std::vector<ECProject::fp>> cp_ptr, ECProject::EncodeType encode_type);

int main(int argc, char const *argv[])
{
	char buff[256];
    getcwd(buff, 256);
    std::string cwf = std::string(argv[0]);

    int k = 10;
    int l = 4;
    int g = 2;
    int objects_num = 1; // <10
    int block_size = 512;
    int value_length = 5;
    ECProject::EncodeType encode_type = ECProject::NU_LRC;
    auto cp_ptr = std::make_shared<std::vector<ECProject::fp>>();
    if(encode_type == ECProject::NU_LRC)
    {
        ECProject::fp tmp1;
        tmp1.ki = 6;
        tmp1.ri = 4;
        cp_ptr->push_back(tmp1);
        ECProject::fp tmp2;
        tmp2.ki = 4;
        tmp2.ri = 2;
        cp_ptr->push_back(tmp2);
        ECProject::fp tmp3;
        tmp3.ki = 4;
        tmp3.ri = 4;
        cp_ptr->push_back(tmp3);
    }
    // test_decodability(k, g, l, cp_ptr, encode_type);

    int parity_blocks_number = g + l;
    std::vector<char *> v_data(k);
    std::vector<char *> v_coding(parity_blocks_number);
    char **data = (char **)v_data.data();
    char **coding = (char **)v_coding.data();
    std::vector<std::vector<char>> coding_area(parity_blocks_number, std::vector<char>(block_size));

    std::string readdir = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../../data/";
    std::string targetdir = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../../storage/";
    if (access(targetdir.c_str(), 0) == -1)
    {
        mkdir(targetdir.c_str(), S_IRWXU);
    }

    char *value = new char[value_length * 1024];

    // Storage
    for(int j = 0; j < objects_num; j++)
    {
        std::string key;
        if (j < 10)
        {
          key = "Object0" + std::to_string(j);
        }
        else
        {
          key = "Object" + std::to_string(j);
        }
        std::string readpath = readdir + key;
        
        std::ifstream ifs(readpath);
        ifs.read(value, value_length * 1024);
        ifs.close();
        char *p_value = const_cast<char *>(value);
        for (int i = 0; i < k; i++)
        {
            data[i] = p_value + i * block_size;
        }
        for (int i = 0; i < parity_blocks_number; i++)
        {
            coding[i] = coding_area[i].data();
        }
        if(encode_type == ECProject::NU_LRC)
        {
            ECProject::encode_Non_Uniform_LRC(k, g, l, data, coding, block_size, cp_ptr);
        }
        else if(encode_type == ECProject::U_LRC)
        {
            ECProject::encode_Uniform_LRC(k, g, l, data, coding, block_size);
        }
        else
        {
            ECProject::encode_Azu_LRC(k, g, l, data, coding, block_size);
        }

        for(int i = 0; i < k + g + l; i++)
        {
            std::string block_key;
            block_key = key + "_" + std::to_string(i);
	        std::string writepath = targetdir + block_key;
	        std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
	        if(i < k)
	        {
	          ofs.write(data[i], block_size);
	        }
	        else
	        {
	          ofs.write(coding[i - k], block_size);
	        }
	        ofs.flush();
	        ofs.close();
        }
    }

    delete value;

    test_decode(k, g, l, block_size, targetdir, cp_ptr, encode_type);
    test_partail_decode_global(k, g, l, block_size, targetdir, cp_ptr, encode_type);
    test_partail_decode_local(k, g, l, block_size, targetdir);

	return 0;
}

void test_decode(int k, int g, int l, int block_size, std::string targetdir, std::shared_ptr<std::vector<ECProject::fp>> cp_ptr, ECProject::EncodeType encode_type)
{
    auto svrs_idx_ptr = std::make_shared<std::vector<int>>();
    auto fls_idx_ptr = std::make_shared<std::vector<int>>();
    fls_idx_ptr->push_back(3);
    fls_idx_ptr->push_back(1);
    fls_idx_ptr->push_back(4);
    svrs_idx_ptr->push_back(8);
    svrs_idx_ptr->push_back(2);
    svrs_idx_ptr->push_back(7);
    svrs_idx_ptr->push_back(6);
    svrs_idx_ptr->push_back(5);
    svrs_idx_ptr->push_back(11);
    svrs_idx_ptr->push_back(15);
    svrs_idx_ptr->push_back(13);
    svrs_idx_ptr->push_back(9);
    svrs_idx_ptr->push_back(12);
    svrs_idx_ptr->push_back(0);
    svrs_idx_ptr->push_back(10);
    svrs_idx_ptr->push_back(14);

    int failed_num = int(fls_idx_ptr->size());
    std::vector<char *> stripe_data(k + g + l);
    char **stripe_blocks = (char **)stripe_data.data();

    // partial 1
    int svrs_num = int(svrs_idx_ptr->size());
    std::vector<char *> svrs_data(svrs_num);
    char **svrs = (char **)svrs_data.data();
    std::vector<std::vector<char>> svrs_data_area(svrs_num, std::vector<char>(block_size));
    int i = 0;
    for(auto it = svrs_idx_ptr->begin(); it != svrs_idx_ptr->end(); it++, i++)
    {
        int idx = *it;
        std::string key;
        key = "Object0" + std::to_string(idx/(k + g + l)) + "_" + std::to_string(idx);
        std::string readpath = targetdir + key;
        svrs[i] = svrs_data_area[i].data();
        stripe_blocks[idx] = svrs[i];
        std::ifstream ifs(readpath);
        ifs.read(svrs[i], block_size);
        ifs.close();
    }
    
    // failed part
    std::vector<char *> failed_data(failed_num);
    char **failed_blocks = (char **)failed_data.data();
    std::vector<std::vector<char>> failed_data_area(failed_num, std::vector<char>(block_size));
    i = 0;
    for(auto it = fls_idx_ptr->begin(); it != fls_idx_ptr->end(); it++, i++)
    {
        int idx = *it;
        std::string key;
        key = "Object0" + std::to_string(idx/(k + g + l)) + "_" + std::to_string(idx);
        std::string readpath = targetdir + key;
        failed_blocks[i] = failed_data_area[i].data();
        stripe_blocks[idx] = failed_blocks[i];
        std::ifstream ifs(readpath);
        ifs.read(failed_blocks[i], block_size);
        ifs.close();
    }

    std::vector<char *> repaired_data(failed_num);
    char **repaired_blocks = (char **)repaired_data.data();
    std::vector<std::vector<char>> repaired_data_area(failed_num, std::vector<char>(block_size));
    for(int j = 0; j < failed_num; j++)
    {
    	repaired_blocks[j] = repaired_data_area[j].data();
    }
    
    int *erasures = new int[failed_num + 1];
    i = 0;
    for(auto it = fls_idx_ptr->begin(); it != fls_idx_ptr->end(); it++, i++)
    {
        int idx = *it;
        erasures[i] = idx;
    }
    erasures[failed_num] = -1;
    printf("\n--- Repaired directly ---\n");
    std::vector<char *> r_data(k);
    char **data_ptrs = (char **)r_data.data();
    std::vector<char *> r_coding(g + l);
    char **coding_ptrs = (char **)r_coding.data();
    for(int i = 0; i < k + g + l; i++)
    {
    	if(i < k)
    	{
    		data_ptrs[i] = stripe_blocks[i];
    	}
    	else
    	{
    		coding_ptrs[i - k] = stripe_blocks[i];
    	}
    	
    	printf("Block %d : %s\n", i, stripe_blocks[i]);
    }
    for(int i = 0; i < failed_num; i++)
    {
    	printf("Block %d before lost:\n%s\n", erasures[i], stripe_blocks[erasures[i]]);
    	memset(stripe_blocks[erasures[i]], 0, block_size);
    	printf("Block %d after lost:\n%s\n", erasures[i], stripe_blocks[erasures[i]]);
    }

    if(encode_type == ECProject::NU_LRC)
    {
        ECProject::decode_Non_Uniform_LRC(k, g, l, data_ptrs, coding_ptrs, block_size, erasures, failed_num, cp_ptr);
    }
    else if(encode_type == ECProject::U_LRC)
    {
        ECProject::decode_Uniform_LRC(k, g, l, data_ptrs, coding_ptrs, block_size, erasures, failed_num);
    }
    else if(encode_type == ECProject::Azu_LRC)
    {
        ECProject::decode_Azu_LRC(k, g, l, data_ptrs, coding_ptrs, block_size, erasures, failed_num);
    }

    for(int i = 0; i < failed_num; i++)
    {
    	printf("Block %d repaired:\n%s\n", erasures[i], stripe_blocks[erasures[i]]);
    }
}

void test_partail_decode_global(int k, int g, int l, int block_size, std::string targetdir, std::shared_ptr<std::vector<ECProject::fp>> cp_ptr, ECProject::EncodeType encode_type)
{
    auto svrs_idx_ptr = std::make_shared<std::vector<int>>();
    auto sls1_idx_ptr = std::make_shared<std::vector<int>>();
    auto sls2_idx_ptr = std::make_shared<std::vector<int>>();
    auto fls_idx_ptr = std::make_shared<std::vector<int>>();
    svrs_idx_ptr->push_back(0);
    svrs_idx_ptr->push_back(2);
    svrs_idx_ptr->push_back(4);
    svrs_idx_ptr->push_back(5);
    svrs_idx_ptr->push_back(9);
    svrs_idx_ptr->push_back(3);
    svrs_idx_ptr->push_back(6);
    svrs_idx_ptr->push_back(7);
    svrs_idx_ptr->push_back(10);
    svrs_idx_ptr->push_back(14);
    fls_idx_ptr->push_back(1);
    fls_idx_ptr->push_back(8);
    sls1_idx_ptr->push_back(9);
    sls1_idx_ptr->push_back(2);
    sls1_idx_ptr->push_back(5);
    sls1_idx_ptr->push_back(4);
    sls1_idx_ptr->push_back(7);
    sls1_idx_ptr->push_back(14);
    sls2_idx_ptr->push_back(6);
    sls2_idx_ptr->push_back(3);
    sls2_idx_ptr->push_back(0);
    sls2_idx_ptr->push_back(10);

    int failed_num = int(fls_idx_ptr->size()), partial_num = 2;
    int partial_blocks_num = failed_num * partial_num;
    std::vector<char *> partial_coding(partial_blocks_num);
    char **partial_blocks = (char **)partial_coding.data();
    std::vector<std::vector<char>> partial_coding_area(partial_blocks_num, std::vector<char>(block_size));
    for(int i = 0; i < partial_blocks_num; i++)
    {
    	partial_blocks[i] = partial_coding_area[i].data();
    }
    std::vector<char *> stripe_data(k + g + l);
    char **stripe_blocks = (char **)stripe_data.data();

    // partial 1
    int sls1_num = int(sls1_idx_ptr->size());
    std::vector<char *> sls1_data(sls1_num);
    char **sls1 = (char **)sls1_data.data();
    std::vector<std::vector<char>> sls1_data_area(sls1_num, std::vector<char>(block_size));
    int i = 0;
    for(auto it = sls1_idx_ptr->begin(); it != sls1_idx_ptr->end(); it++, i++)
    {
        int idx = *it;
        std::string key;
        key = "Object0" + std::to_string(idx/(k + g + l)) + "_" + std::to_string(idx);
        std::string readpath = targetdir + key;
        sls1[i] = sls1_data_area[i].data();
        stripe_blocks[idx] = sls1[i];
        std::ifstream ifs(readpath);
        ifs.read(sls1[i], block_size);
        ifs.close();
    }
    printf("Partial 1: encoding partial blocks!\n");
    if(encode_type == ECProject::NU_LRC)
    {
        ECProject::encode_partial_blocks_for_decoding_Non_Uniform_LRC(k, g, l, sls1, partial_blocks, block_size, sls1_idx_ptr, svrs_idx_ptr, fls_idx_ptr, cp_ptr);
    }
    else if(encode_type == ECProject::U_LRC)
    {
        ECProject::encode_partial_blocks_for_decoding_Uniform_LRC(k, g, l, sls1, partial_blocks, block_size, sls1_idx_ptr, svrs_idx_ptr, fls_idx_ptr);
    }
    else if(encode_type == ECProject::Azu_LRC)
    {
        ECProject::encode_partial_blocks_for_decoding_Azu_LRC(k, g, l, sls1, partial_blocks, block_size, sls1_idx_ptr, svrs_idx_ptr, fls_idx_ptr);
    }

    // partial 2
    int sls2_num = int(sls2_idx_ptr->size());
    std::vector<char *> sls2_data(sls2_num);
    char **sls2 = (char **)sls2_data.data();
    std::vector<std::vector<char>> sls2_data_area(sls2_num, std::vector<char>(block_size));
    i = 0;
    for(auto it = sls2_idx_ptr->begin(); it != sls2_idx_ptr->end(); it++, i++)
    {
        int idx = *it;
        std::string key;
        key = "Object0" + std::to_string(idx/(k + g + l)) + "_" + std::to_string(idx);
        std::string readpath = targetdir + key;
        sls2[i] = sls2_data_area[i].data();
        stripe_blocks[idx] = sls2[i];
        std::ifstream ifs(readpath);
        ifs.read(sls2[i], block_size);
        ifs.close();
    }
    printf("Partial 2: encoding partial blocks!\n");
    if(encode_type == ECProject::NU_LRC)
    {
        ECProject::encode_partial_blocks_for_decoding_Non_Uniform_LRC(k, g, l, sls2, &partial_blocks[failed_num], block_size, sls2_idx_ptr, svrs_idx_ptr, fls_idx_ptr, cp_ptr);
    }
    else if(encode_type == ECProject::U_LRC)
    {
        ECProject::encode_partial_blocks_for_decoding_Uniform_LRC(k, g, l, sls2, &partial_blocks[failed_num], block_size, sls2_idx_ptr, svrs_idx_ptr, fls_idx_ptr);
    }
    else if(encode_type == ECProject::Azu_LRC)
    {
        ECProject::encode_partial_blocks_for_decoding_Azu_LRC(k, g, l, sls2, &partial_blocks[failed_num], block_size, sls2_idx_ptr, svrs_idx_ptr, fls_idx_ptr);
    }

    // failed part
    std::vector<char *> failed_data(failed_num);
    char **failed_blocks = (char **)failed_data.data();
    std::vector<std::vector<char>> failed_data_area(failed_num, std::vector<char>(block_size));
    i = 0;
    for(auto it = fls_idx_ptr->begin(); it != fls_idx_ptr->end(); it++, i++)
    {
        int idx = *it;
        std::string key;
        key = "Object0" + std::to_string(idx/(k + g + l)) + "_" + std::to_string(idx);
        std::string readpath = targetdir + key;
        failed_blocks[i] = failed_data_area[i].data();
        stripe_blocks[idx] = failed_blocks[i];
        std::ifstream ifs(readpath);
        ifs.read(failed_blocks[i], block_size);
        ifs.close();
    }

    std::vector<char *> repaired_data(failed_num);
    char **repaired_blocks = (char **)repaired_data.data();
    std::vector<std::vector<char>> repaired_data_area(failed_num, std::vector<char>(block_size));
    for(int j = 0; j < failed_num; j++)
    {
    	repaired_blocks[j] = repaired_data_area[j].data();
    }
    printf("Decoding\n");
    ECProject::perform_addition(partial_blocks, repaired_blocks, block_size, partial_blocks_num, failed_num);

    printf("\n--- Applying encode-and-transfer ---\n");
    i = 0;
    for(auto it = fls_idx_ptr->begin(); it != fls_idx_ptr->end(); it++, i++)
    {
        int idx = *it;
        printf("\nLost block %d:\n", idx);
        printf("Before lost: %s\n", failed_blocks[i]);
        printf("After repair: %s\n", repaired_blocks[i]);
    }
}

void test_partail_decode_local(int k, int g, int l, int block_size, std::string targetdir)
{
    auto sls1_idx_ptr = std::make_shared<std::vector<int>>();
    auto sls2_idx_ptr = std::make_shared<std::vector<int>>();
    auto fls_idx_ptr = std::make_shared<std::vector<int>>();
    fls_idx_ptr->push_back(5);
    sls1_idx_ptr->push_back(4);
    sls1_idx_ptr->push_back(10); 
    sls2_idx_ptr->push_back(15);
    sls2_idx_ptr->push_back(11);

    int failed_num = int(fls_idx_ptr->size()), partial_num = 2;
    int partial_blocks_num = failed_num * partial_num;
    std::vector<char *> partial_coding(partial_blocks_num);
    char **partial_blocks = (char **)partial_coding.data();
    std::vector<std::vector<char>> partial_coding_area(partial_blocks_num, std::vector<char>(block_size));
    for(int i = 0; i < partial_blocks_num; i++)
    {
    	partial_blocks[i] = partial_coding_area[i].data();
    }
    std::vector<char *> stripe_data(k + g + l);
    char **stripe_blocks = (char **)stripe_data.data();

    // partial 1
    int sls1_num = int(sls1_idx_ptr->size());
    std::vector<char *> sls1_data(sls1_num);
    char **sls1 = (char **)sls1_data.data();
    std::vector<std::vector<char>> sls1_data_area(sls1_num, std::vector<char>(block_size));
    int i = 0;
    for(auto it = sls1_idx_ptr->begin(); it != sls1_idx_ptr->end(); it++, i++)
    {
        int idx = *it;
        std::string key;
        key = "Object0" + std::to_string(idx/(k + g + l)) + "_" + std::to_string(idx);
        std::string readpath = targetdir + key;
        sls1[i] = sls1_data_area[i].data();
        stripe_blocks[idx] = sls1[i];
        std::ifstream ifs(readpath);
        ifs.read(sls1[i], block_size);
        ifs.close();
    }
    printf("Partial 1: encoding partial blocks!\n");
    ECProject::perform_addition(sls1, partial_blocks, block_size, sls1_num, 1);
    
    // partial 2
    int sls2_num = int(sls2_idx_ptr->size());
    std::vector<char *> sls2_data(sls2_num);
    char **sls2 = (char **)sls2_data.data();
    std::vector<std::vector<char>> sls2_data_area(sls2_num, std::vector<char>(block_size));
    i = 0;
    for(auto it = sls2_idx_ptr->begin(); it != sls2_idx_ptr->end(); it++, i++)
    {
        int idx = *it;
        std::string key;
        key = "Object0" + std::to_string(idx/(k + g + l)) + "_" + std::to_string(idx);
        std::string readpath = targetdir + key;
        sls2[i] = sls2_data_area[i].data();
        stripe_blocks[idx] = sls2[i];
        std::ifstream ifs(readpath);
        ifs.read(sls2[i], block_size);
        ifs.close();
    }
    printf("Partial 2: encoding partial blocks!\n");
    ECProject::perform_addition(sls2, &partial_blocks[1], block_size, sls2_num, 1);

    // failed part
    std::vector<char *> failed_data(failed_num);
    char **failed_blocks = (char **)failed_data.data();
    std::vector<std::vector<char>> failed_data_area(failed_num, std::vector<char>(block_size));
    i = 0;
    for(auto it = fls_idx_ptr->begin(); it != fls_idx_ptr->end(); it++, i++)
    {
        int idx = *it;
        std::string key;
        key = "Object0" + std::to_string(idx/(k + g + l)) + "_" + std::to_string(idx);
        std::string readpath = targetdir + key;
        failed_blocks[i] = failed_data_area[i].data();
        stripe_blocks[idx] = failed_blocks[i];
        std::ifstream ifs(readpath);
        ifs.read(failed_blocks[i], block_size);
        ifs.close();
    }

    std::vector<char *> repaired_data(failed_num);
    char **repaired_blocks = (char **)repaired_data.data();
    std::vector<std::vector<char>> repaired_data_area(failed_num, std::vector<char>(block_size));
    for(int j = 0; j < failed_num; j++)
    {
    	repaired_blocks[j] = repaired_data_area[j].data();
    }
    printf("Decoding\n");
    ECProject::perform_addition(partial_blocks, repaired_blocks, block_size, partial_num, 1);

    printf("\n--- Applying encode-and-transfer ---\n");
    i = 0;
    for(auto it = fls_idx_ptr->begin(); it != fls_idx_ptr->end(); it++, i++)
    {
        int idx = *it;
        printf("\nLost block %d:\n", idx);
        printf("Before lost: %s\n", failed_blocks[i]);
        printf("After repair: %s\n", repaired_blocks[i]);
    }
}

void test_decodability(int k, int g, int l, std::shared_ptr<std::vector<ECProject::fp>> cp_ptr, ECProject::EncodeType encode_type)
{
    bool test = true;
    if(test)
    {
        std::vector<int> nums = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18};
        int m = 5;
        int n = 19;
        std::vector<bool> bitmask(n, false);
        for (int i = 0; i < m; ++i) {
            bitmask[i] = true;
        }

        int tot_cnt = 0;
        int f_cnt = 0;
        do {
            auto fls_idx_ptr = std::make_shared<std::vector<int>>();
            for (int i = 0; i < n; ++i) {
                if (bitmask[i]) {
                    fls_idx_ptr->push_back(nums[i]);
                }
            }
            bool flag = false;
            if(encode_type == ECProject::NU_LRC)
            {
                flag = ECProject::check_if_decodable_Non_Uniform_LRC(k, g, l, cp_ptr, fls_idx_ptr);
            }
            else if(encode_type == ECProject::U_LRC)
            {
                flag = ECProject::check_if_decodable_Uniform_LRC(k, g, l, fls_idx_ptr);
            }
            else if(encode_type == ECProject::Azu_LRC)
            {
                flag = ECProject::check_if_decodable_Azu_LRC(k, g, l, fls_idx_ptr);
            }
            if(flag)
            {
                std::cout << "[^-^] Decodable!  ";
            }
            else
            {
                f_cnt += 1;
                std::cout << "[T_T] Undecodable!  ";
            }
            for(auto it = fls_idx_ptr->begin(); it != fls_idx_ptr->end(); it++)
            {
                std::cout << *it << " ";
            }
            std::cout << std::endl;
            tot_cnt += 1;
        } while (std::prev_permutation(bitmask.begin(), bitmask.end()));
        std::cout << "Result: [Total]" << tot_cnt << ", [Failed]" << f_cnt << std::endl;
    }
    else
    {
        auto fls_idx_ptr = std::make_shared<std::vector<int>>();
        fls_idx_ptr->push_back(5);
        fls_idx_ptr->push_back(2);
        fls_idx_ptr->push_back(0);
        fls_idx_ptr->push_back(7);
        fls_idx_ptr->push_back(1);
        fls_idx_ptr->push_back(4);
        bool flag = false;
        if(encode_type == ECProject::NU_LRC)
        {
            flag = ECProject::check_if_decodable_Non_Uniform_LRC(k, g, l, cp_ptr, fls_idx_ptr);
        }
        else if(encode_type == ECProject::U_LRC)
        {
            flag = ECProject::check_if_decodable_Uniform_LRC(k, g, l, fls_idx_ptr);
        }
        else if(encode_type == ECProject::Azu_LRC)
        {
            flag = ECProject::check_if_decodable_Azu_LRC(k, g, l, fls_idx_ptr);
        }
        if(flag)
        {
            std::cout << "[^-^] Decodable!" << std::endl;
        }
        else
        {
            std::cout << "[T_T] Undecodable!" << std::endl;
        }
    }
}