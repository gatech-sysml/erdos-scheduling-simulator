class ProfileManager():
    def __init__(self):
        self._fifo = dict(quality = None, runtime = None, num_invocations = 0)
        self._edf = dict(quality = None, runtime = None, num_invocations = 0)
        self._lsf = dict(quality = None, runtime = None, num_invocations = 0)
        self._z3 = dict(quality = None, runtime = None, num_invocations = 0)
        self._branch_prediction = dict(quality = None, runtime = None, num_invocations = 0)
        self._ilp = dict(quality = None, runtime = None, num_invocations = 0)
        self._tetrisched_cplex = dict(quality = None, runtime = None, num_invocations = 0)
        self._tetrisched_gurobi = dict(quality = None, runtime = None, num_invocations = 0)
        self._clockwork = dict(quality = None, runtime = None, num_invocations = 0)
        self._tetrisched = dict(quality = None, runtime = None, num_invocations = 0)
        self._graphene_prime = dict(quality = None, runtime = None, num_invocations = 0)
        self._graphene = dict(quality = None, runtime = None, num_invocations = 0)

    @property
    def fifo(self):
        return self._fifo
    
    @fifo.setter
    def fifo(self, quality, runtime):
        curr_quality = self._fifo["quality"]
        curr_runtime = self._fifo["runtime"]
        num_invocations = self._fifo["num_invocations"]
        # maintain running average of quality and runtime
        self._fifo["quality"] = (curr_quality * num_invocations + quality) / (num_invocations + 1)
        self._fifo["runtime"] = (curr_runtime * num_invocations + runtime) / (num_invocations + 1)
        self._fifo["num_invocations"] = num_invocations + 1

    @property
    def edf(self):
        return self._edf
    
    @edf.setter
    def edf(self, quality, runtime):
        curr_quality = self._edf["quality"]
        curr_runtime = self._edf["runtime"]
        num_invocations = self._edf["num_invocations"]
        self._edf["quality"] = (curr_quality * num_invocations + quality) / (num_invocations + 1)
        self._edf["runtime"] = (curr_runtime * num_invocations + runtime) / (num_invocations + 1)
        self._edf["num_invocations"] = num_invocations + 1

    @property
    def lsf(self):
        return self._lsf
    
    @lsf.setter
    def lsf(self, quality, runtime):
        curr_quality = self._lsf["quality"]
        curr_runtime = self._lsf["runtime"]
        num_invocations = self._lsf["num_invocations"]
        self._lsf["quality"] = (curr_quality * num_invocations + quality) / (num_invocations + 1)
        self._lsf["runtime"] = (curr_runtime * num_invocations + runtime) / (num_invocations + 1)
        self._lsf["num_invocations"] = num_invocations + 1

    @property
    def z3(self):
        return self._z3
    
    @z3.setter
    def z3(self, quality, runtime):
        curr_quality = self._z3["quality"]
        curr_runtime = self._z3["runtime"]
        num_invocations = self._z3["num_invocations"]
        self._z3["quality"] = (curr_quality * num_invocations + quality) / (num_invocations + 1)
        self._z3["runtime"] = (curr_runtime * num_invocations + runtime) / (num_invocations + 1)
        self._z3["num_invocations"] = num_invocations + 1

    @property
    def branch_prediction(self):
        return self._branch_prediction
    
    @branch_prediction.setter
    def branch_prediction(self, quality, runtime):
        curr_quality = self._branch_prediction["quality"]
        curr_runtime = self._branch_prediction["runtime"]
        num_invocations = self._branch_prediction["num_invocations"]
        self._branch_prediction["quality"] = (curr_quality * num_invocations + quality) / (num_invocations + 1)
        self._branch_prediction["runtime"] = (curr_runtime * num_invocations + runtime) / (num_invocations + 1)
        self._branch_prediction["num_invocations"] = num_invocations + 1

    @property
    def ilp(self):
        return self._ilp
    
    @ilp.setter
    def ilp(self, quality, runtime):
        curr_quality = self._ilp["quality"]
        curr_runtime = self._ilp["runtime"]
        num_invocations = self._ilp["num_invocations"]
        self._ilp["quality"] = (curr_quality * num_invocations + quality) / (num_invocations + 1)
        self._ilp["runtime"] = (curr_runtime * num_invocations + runtime) / (num_invocations + 1)
        self._ilp["num_invocations"] = num_invocations + 1

    @property
    def tetrisched_cplex(self):
        return self._tetrisched_cplex
    
    @tetrisched_cplex.setter
    def tetrisched_cplex(self, quality, runtime):
        curr_quality = self._tetrisched_cplex["quality"]
        curr_runtime = self._tetrisched_cplex["runtime"]
        num_invocations = self._tetrisched_cplex["num_invocations"]
        self._tetrisched_cplex["quality"] = (curr_quality * num_invocations + quality) / (num_invocations + 1)
        self._tetrisched_cplex["runtime"] = (curr_runtime * num_invocations + runtime) / (num_invocations + 1)
        self._tetrisched_cplex["num_invocations"] = num_invocations + 1

    @property
    def tetrisched_gurobi(self):
        return self._tetrisched_gurobi
    
    @tetrisched_gurobi.setter
    def tetrisched_gurobi(self, quality, runtime):
        curr_quality = self._tetrisched_gurobi["quality"]
        curr_runtime = self._tetrisched_gurobi["runtime"]
        num_invocations = self._tetrisched_gurobi["num_invocations"]
        self._tetrisched_gurobi["quality"] = (curr_quality * num_invocations + quality) / (num_invocations + 1)
        self._tetrisched_gurobi["runtime"] = (curr_runtime * num_invocations + runtime) / (num_invocations + 1)
        self._tetrisched_gurobi["num_invocations"] = num_invocations + 1

    @property
    def clockwork(self):
        return self._clockwork
    
    @clockwork.setter
    def clockwork(self, quality, runtime):
        curr_quality = self._clockwork["quality"]
        curr_runtime = self._clockwork["runtime"]
        num_invocations = self._clockwork["num_invocations"]
        self._clockwork["quality"] = (curr_quality * num_invocations + quality) / (num_invocations + 1)
        self._clockwork["runtime"] = (curr_runtime * num_invocations + runtime) / (num_invocations + 1)
        self._clockwork["num_invocations"] = num_invocations + 1

    @property
    def tetrisched(self):
        return self._tetrisched
    
    @tetrisched.setter
    def tetrisched(self, quality, runtime):
        curr_quality = self._tetrisched["quality"]
        curr_runtime = self._tetrisched["runtime"]
        num_invocations = self._tetrisched["num_invocations"]
        self._tetrisched["quality"] = (curr_quality * num_invocations + quality) / (num_invocations + 1)
        self._tetrisched["runtime"] = (curr_runtime * num_invocations + runtime) / (num_invocations + 1)
        self._tetrisched["num_invocations"] = num_invocations + 1

    @property
    def graphene_prime(self):
        return self._graphene_prime
    
    @graphene_prime.setter
    def graphene_prime(self, quality, runtime):
        curr_quality = self._graphene_prime["quality"]
        curr_runtime = self._graphene_prime["runtime"]
        num_invocations = self._graphene_prime["num_invocations"]
        self._graphene_prime["quality"] = (curr_quality * num_invocations + quality) / (num_invocations + 1)
        self._graphene_prime["runtime"] = (curr_runtime * num_invocations + runtime) / (num_invocations + 1)
        self._graphene_prime["num_invocations"] = num_invocations + 1

    @property
    def graphene(self):
        return self._graphene
    
    @graphene.setter
    def graphene(self, quality, runtime):
        curr_quality = self._graphene["quality"]
        curr_runtime = self._graphene["runtime"]
        num_invocations = self._graphene["num_invocations"]
        self._graphene["quality"] = (curr_quality * num_invocations + quality) / (num_invocations + 1)
        self._graphene["runtime"] = (curr_runtime * num_invocations + runtime) / (num_invocations + 1)
        self._graphene["num_invocations"] = num_invocations + 1

    def profiles(self):
        return dict(fifo = self._fifo, edf = self._edf, lsf = self._lsf, z3 = self._z3, 
                    branch_prediction = self._branch_prediction, ilp = self._ilp, 
                    tetrisched_cplex = self._tetrisched_cplex, tetrisched_gurobi = self._tetrisched_gurobi, 
                    clockwork = self._clockwork, tetrisched = self._tetrisched, 
                    graphene_prime = self._graphene_prime, graphene = self._graphene)
    

class MetricManager():
    def __init__(self):
        self._utilization = dict(utilization = None, sim_time = 0)

    @property
    def utilization(self):
        return self._utilization

    @utilization.setter
    def utilization(self, utilization):
        self._utilization = utilization

    def percent_utilization(self):
        allocated_resources = 0
        available_resources = 0

        for pool_utilization in self._utilization:
            for res_utilization in pool_utilization:
                allocated_resources += res_utilization["resource_allocation"]
                available_resources += res_utilization["resource_availability"]

        return (allocated_resources * 1.0) / (allocated_resources + available_resources)

    def metrics(self):
        utilz = self.percent_utilization()
        return dict(utilization = utilz)