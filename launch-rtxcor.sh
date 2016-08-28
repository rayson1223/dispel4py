#python -m dispel4py.new.processor simple test.rtxcorr.rtxcor2 -f xcrr-input

python -m dispel4py.new.processor multi test.rtxcorr.rtxcor2 -n 34 -f xcrr-input

#mpiexec-openmpi-mp -n 24 python -m dispel4py.new.processor mpi test.rtxcorr.rtxcorr -f xcrr-input

#ulimit -n 5200
#python test/rtxcorr/rtxcor2.py mpi