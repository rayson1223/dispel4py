#python -m dispel4py.new.processor simple undetermined-tst 
python -m dispel4py.new.processor multi test.feedbackloop.feedbackloop -n 20 -f undetermined-input

#mpiexec-openmpi-mp python -m dispel4py.new.processor multi feedbackloop -n 12 -f undetermined-input 
