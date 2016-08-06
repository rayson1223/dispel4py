
# coding: utf-8

# ## Enabling Active-Provenance in dispel4py - How To:
# 
# 
# 
# ### Sample Workflow Description and Components (Compoenents are implemented as Classe or as Functions)
# <br/>
# 
# <li>1 - Produces a stream of numbers - Source (Class)</li>
# <li>2 - Calculates the square-product for each number - square (fuction)</li>
# <li>3 - Streams out the cartesian product of (numbers X square-products) - CrossProd (Class)</li>
# <li>4 - Divides (square-product/number) for each incoming element of the cartesian product - Div (Class)</li>
# 
# The script below defines the components and declares the workflow. Its execution will show a visual representation of the abstract workfkow grap.
# 
# While most of the processing elements are stateless, the <i>CrossProd</i> Class shows how to use the <i>write</i> method and the <i>addToState</i> to precisely trace stateful dependencies, in the case of the generation of a cross-product output.
# 
# ### Function addToProvState 
# Adds an object and its metadata to the PEs state. This can be referenced from the user during write operations, increasing the lineage precision in stateful components.
# 
# The accepted parameters are the following:
# 
# #### Unnamed parameters:
# <li> 1 - <i>data</i>: object to be stored in the provenance state</li>
# 
# #### Named Parameters:
# <li> 2 - <i>location</i>: url or path indicating the location of the data file, if any has been produced</li>
# <li> 3 - <i>metadata</i>: dictionary of key,values pairs od user-defined metadata associated to the object.</li>
# <li> 4 - <i>ignore_dep</i>: If <b>True</b> the dependencies which are currently standing are ignored, default True</li>
# <li> 5 - <i>stateless</i>:  If <b>True</b> the item added is not included as new standing dependencies, default True</li>
# 
# 
# 
# <br/>
# 

# In[7]:

from dispel4py.workflow_graph import WorkflowGraph 
from dispel4py.provenance import *
from dispel4py.new.processor  import *
import time
import random
import numpy
import traceback 
from dispel4py.base import create_iterative_chain, GenericPE, ConsumerPE, IterativePE, SimpleFunctionPE
from dispel4py.new.simple_process import process_and_return

import IPython
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats.stats import pearsonr 


sns.set(style="white")


class Start(GenericPE):

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('iterations')
        self._add_output('output')
        #self.prov_cluster="myne"
    
    def _process(self,inputs):
        
        if 'iterations' in inputs:
            inp=inputs['iterations']
            #self.log(inp[0])
            self.write('output',inp,metadata={'val':inp})
            #self.write('provenance',inp,metadata={'val':'inp'})
        
        #Uncomment this line to associate this PE to the mycluster provenance-cluster 
        #self.prov_cluster ='mycluster'

class Source(GenericPE):

    def __init__(self,sr):
        GenericPE.__init__(self)
        self._add_input('iterations')
        self._add_output('output')
        self.sr=sr
        #self.prov_cluster="myne"
         
        self.parameters={'sampling_rate':sr}
        
        #Uncomment this line to associate this PE to the mycluster provenance-cluster 
        #self.prov_cluster ='mycluster'
        
    
    def _process(self,inputs):
        #print inputs
        #inp=0
        if 'iterations' in inputs:
            inp=inputs['iterations'][0]
       
        #Strarting from the number received in input, streams out values until >0
        while (inp>0):
            val=random.uniform(0,100)
            time.sleep(1/self.sr)
            self.write('output',val,metadata={'val':val})
            inp-=1
        
        
        

class PlotData(GenericPE):

    def __init__(self,variables_number):
        GenericPE.__init__(self)
        #input added by parametrisation of number of variables
        #self._add_input('input1',grouping=[1])
        #self._add_input('input2',grouping=[1])
        #self._add_input('input3',grouping=[1])
        self._add_output('output')
        self.size=variables_number
        self.parameters={'variables_number':variables_number}
        self.data={}
         
        
        #Uncomment this line to associate this PE to the mycluster provenance-cluster 
        #self.prov_cluster ='mycluster'self.prov_cluster='mycluster'
            
    def _process(self,data):
        for x in data:
            #IPython.display.clear_output()
            if data[x][1] not in self.data:
                self.data[data[x][1]]={}
                self.data[data[x][1]]['matrix']=numpy.identity(self.size)
                self.data[data[x][1]]['ro_count']=0
            #self.data[data[x][1]]['matrix'][data[x][2]]=data[x][0]
            self.data[data[x][1]]['matrix'][(data[x][2][1],data[x][2][0])]=data[x][0]
            self.data[data[x][1]]['ro_count']+=1
            #self.log(self.data[data[x][1]]['ro_count'])
            #self.log(x)
            if self.data[data[x][1]]['ro_count']==(self.size*(self.size-1))/2:
                
                d = pd.DataFrame(data=self.data[data[x][1]]['matrix'],
                 columns=range(0,self.size),index=range(0,self.size))
                
                mask = numpy.zeros_like(d, dtype=numpy.bool)
                mask[numpy.triu_indices_from(mask)] = True

                # Set up the matplotlib figure
                f, ax = plt.subplots(figsize=(11, 9))

                # Generate a custom diverging colormap
                cmap = sns.diverging_palette(220, 10, as_cmap=True)

                # Draw the heatmap with the mask and correct aspect ratio
                sns.heatmap(d, mask=mask, cmap=cmap, vmax=1,
                    square=True,
                    linewidths=.5, cbar_kws={"shrink": .5}, ax=ax)
                
                sns.plt.show()   
                
                self.write('output',d,metadata={'matrix':str(d),'batch':str(data[x][1])})
                #IPython.display.display(plt.gcf())

            
class CrossProd(GenericPE):

    def __init__(self,batch_size,index):
        GenericPE.__init__(self)
        self._add_input('input1')
        self._add_input('input2')
        self._add_output('output')
        self.index1=0
        self.index2=0
        self.batch1=[]
        self.batch2=[]
        self.size=batch_size
        self.parameters={'batch_size':batch_size}
        self.index=index
        self.batchnum=0
         
        
    def _process(self, inputs):
        index=None
        val=None
              
            
        try:
            val = inputs['input1']
            self.batch1.append(val)
            
                 
        except KeyError:
            #traceback.print_exc(file=sys.stderr)
            val = inputs['input2']
            self.batch2.append(val)
        
        
        #self.addToProvState(None,,ignore_dep=False)
            
        if len(self.batch2)>=self.size and len(self.batch1)>=self.size:
            array1=numpy.array(self.batch1[0:self.size-1])
            array2=numpy.array(self.batch2[0:self.size-1])
            ro=numpy.corrcoef([array1,array2])
            self.write('output',(ro[0][1],self.batchnum,self.index),metadata={'batchnum':self.batchnum,'ro':str(ro[0][1]),'lenbatch1':str(array1),'lenbatch2':str(array2)})
            self.batchnum+=1
            self.batch1=self.batch1[(self.size-1):len(self.batch1)]
            self.batch2=self.batch2[(self.size-1):len(self.batch2)]
            






# ### Preparing workflow inputs and parameters
# 
# <b>number of projections</b> = <i>iterations/batch_size</i> at speed defined by <i>sampling_rate<i>

# In[8]:

#####################################################################################################

#Declare workflow inputs: (each iteration prduces a batch_size of samples at the specified sampling_rate)
# number of projections = iterations/batch_size at speed defined by sampling rate
variables_number=10
sampling_rate=100
batch_size=5
iterations=15

input_data = {"Source": [{"iterations": [iterations]}]}
      
# Instantiates the Workflow Components  
# and generates the graph based on parameters

sources={}
graph = WorkflowGraph()
plot=PlotData(variables_number)
start=Start()
start2=Start()    

rec=ProvenanceRecorderToServiceBulk()
rec._add_input("2222")
start._add_output("provenance")
#graph.connect(start,'xx',rec,'2222')


for i in range(0,variables_number):
    sources[i] = Source(sampling_rate)

for h in range(0,variables_number):
    graph.connect(start,'output',sources[h],'iterations')
    for j in range(h+1,variables_number):
        cc=CrossProd(batch_size,(h,j))
        cc.numprocesses=1
        plot._add_input('input'+'_'+str(h)+'_'+str(j),grouping=[1])
        graph.connect(sources[h],'output',cc,'input1')
        graph.connect(sources[j],'output',cc,'input2')
        graph.connect(cc,'output',plot,'input'+'_'+str(h)+'_'+str(j))
        

#squaref=SimpleFunctionPE(square,{})
#Uncomment this line to associate this PE to the mycluster provenance-cluster 
#squaref=SimpleFunctionPE(square,{'prov_cluster':'mycluster'})


#Initialise and compose the workflow graph

#graph.connect(scX,'output',crossp1,'input1')
#graph.connect(scY,'output',crossp1,'input2')
#graph.connect(scX,'output',crossp2,'input1')
#graph.connect(scZ,'output',crossp2,'input2')
#graph.connect(scY,'output',crossp3,'input1')
#graph.connect(scZ,'output',crossp3,'input2')
#graph.connect(crossp1,'output',plot,'input1')
#graph.connect(crossp2,'output',plot,'input2')
#graph.connect(crossp3,'output',plot,'input3')

#graph.connect(corf,'output',plot,'input')




#Visualise the graph
from dispel4py.visualisation import display
display(graph)

print ("Preparing for: "+str(iterations/batch_size)+" projections" )


# ## Preparing the workflow graph for provenance production, pre-analysis and storage
# 
# This snippet will make sure that the workflow compoentns will be provenance-aware and the lineage information sent to the designated ProvenanceRecorders for in-workflow pre-analysis.
# 
# The execution will show a new graph where it will be possible to validate the provenance-cluster, if any, and the correct association of ProvenanceRecorders and feedback connections.
# 
# The graph will change according to the declaration of self.prov_cluster property of the processing elements and to the specification of different ProvenanceRecorders and feedback loops, as described below:
# 
# ### Function InitiateNewRun 
# Prepares the workflow with the required provenance mechanisms
# The accepted parameters are the following:
# 
# #### Unnamed parameters:
# <li> 1 - <i>worfklow graph</i></li>
# <li> 2 - Class name implementing the default <i>ProvenanceRecorder</i></li>
# 
# #### Named Parameters
# <li> 3 - <i>provImpClass</i>: Class name extending the default <i>ProvenancePE</i>. The current type of the workflow components (GenericPE) will be extended with the one indicated by the <i>provImpClass</i> type</li>
# <li> 4 - <i>username</i></li>
# <li> 5 - <i>runId</i></li>
# <li> 6 - <i>w3c_prov</i>: specifies if the PE will outupt lineage in PROV format (default=False)</li>
# <li> 7 - <i>workflowName</i></li>
# <li> 8 - <i>workflowId</i></li>
# <li> 9 - <i>clustersRecorders</i>: dictionary associating <i>provenance-clusters</i> with a specific <i>ProvenanceRecorder</i> (overrides the default <i>ProvenanceRecorder</i>) </li>
# <li> 10 - <i>feedbackPEs</i>: list of PE names receiving and processing feedbacks from the <i>ProvenanceRecorder</i>. </li>
# 
# <br/>
# 
# 
# 

# In[9]:

#Location of the remote repository for runtime updates of the lineage traces. Shared among ProvenanceRecorder subtypes
ProvenanceRecorder.REPOS_URL='http://localhost/prov/workflow/insert'

# Ranomdly generated unique identifier for the current run
rid='RDWD_'+getUniqueId()

# if ProvenanceRecorderToFile is used, this path will contains all the resulting JSON documents
os.environ['PROV_PATH']="./prov-files/"

#from dispel4py.new.processor import *
#create_partitioned(graph)
# Finally, provenance enhanced graph is prepared:
InitiateNewRun(graph,ProvenanceRecorderToServiceBulk,provImpClass=ProvenancePE,username='aspinuso',runId=rid,w3c_prov=False,workflowName="test_rdwd",workflowId="xx")


create_partitioned(graph)
#.. and visualised..
#display(graph)


# ### Execution
# The followin instruction executes the workfklow in single-process mode
# <br/>

# In[ ]:

#Launch in simple process
#process_and_return(graph, input_data)


# ## Developing ProvenanceRecorders
# 
# The Class below show a sample <i>ProvenanceRecorderToService</i> and a slightlty more advanced one that allows for feedback.
# 
# ### ProvenanceRecorderToService
# 
# Recieves traces from the PEs and sends them out to an exteranal provenance store.
# 
# 

# In[ ]:


