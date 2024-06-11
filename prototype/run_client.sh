TT=false
ET='NU_LRC'
GG=2
BS=1024
SN=20
XX=1.25
TF='example_workload.txt'

# run client
./project/cmake/build/run_client ${TT} false ${ET} Flat ${GG} ${XX} ${BS} ${SN} false ${TF}
./project/cmake/build/run_client ${TT} true ${ET} Opt ${GG} ${XX} ${BS} ${SN} false ${TF}


# unlimit bandwidth
sh exp.sh 4
# # kill datanodes and proxies
sh exp.sh 0
# # kill coordinator
pkill -9 run_coordinator
