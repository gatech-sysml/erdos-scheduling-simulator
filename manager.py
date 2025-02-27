from scheduler_utils import SchedulerType
from simulator import Simulator
from workload import Resource
from utils import EventTime

class ProfileManager():
    def __init__(self, simulator: Simulator):
        self._simulator = simulator
        self._scheduler_stats = dict()

    def get_profile(self, scheduler_type: SchedulerType):
        return self._scheduler_stats[f"{scheduler_type.name.lower}"]
    
    def set_profile(self, scheduler_type: SchedulerType, quality, runtime):
        scheduler_type = scheduler_type.name.lower()

        if scheduler_type not in self._scheduler_stats:
            self._scheduler_stats[f"{scheduler_type}"] = dict(quality = 0, runtime = 0, num_invocations = 0)

        temp_sstats = self._scheduler_stats[f"{scheduler_type}"]

        curr_quality = temp_sstats["quality"]
        curr_runtime = temp_sstats["runtime"]
        num_invocations = temp_sstats["num_invocations"]

        # maintain running average of quality and runtime
        temp_sstats["quality"] = (curr_quality * num_invocations + quality) / (num_invocations + 1)
        temp_sstats["runtime"] = (curr_runtime * num_invocations + runtime) / (num_invocations + 1)
        temp_sstats["num_invocations"] = num_invocations + 1

    def profiles(self):
        return self._scheduler_stats
    

class MetricManager():
    def __init__(self, simulator: Simulator):
        self._simulator = simulator
        self._utilization = dict(utilization = None, sim_time = 0)

    @property
    def utilization(self):
        return self._utilization

    @utilization.setter
    def utilization(self, utilization):
        self._utilization = utilization

    def update_metrics(self, sim_time: EventTime):
        """Updates the metrics like utilization of the resources of a particular WorkerPool.

        Args:
            sim_time (`EventTime`): The simulation time at which the utilization
                is logged (in us).
        """
        assert (
            sim_time.unit == EventTime.Unit.US
        ), "The simulator time was not in microseconds."
        
        # utilization
        # Cumulate the resources from all the WorkerPools
        total_utilization = list()

        for worker_pool in self._simulator._worker_pools.worker_pools:
            pool_utilization = list()
            
            worker_pool_resources = worker_pool.resources
            for resource_name in set(
                map(lambda value: value[0].name, worker_pool_resources.resources)
            ):
                resource = Resource(name=resource_name, _id="any")
                resource_utilization = dict(resource_name = resource_name, 
                                   resource_allocation = worker_pool_resources.get_allocated_quantity(resource),
                                   resource_availability = worker_pool_resources.get_available_quantity(resource))
                pool_utilization.append(resource_utilization)

            pool_utilization_dict = dict(worker_pool_id = f"{worker_pool.id}", pool_utilization = pool_utilization)
            total_utilization.append(pool_utilization_dict)

        self._utilization = dict(utilization = total_utilization, sim_time = sim_time)

    def __percent_utilization(self):
        allocated_resources = 0
        available_resources = 0

        for pool_utilization in self._utilization["utilization"]:
            for res_utilization in pool_utilization["pool_utilization"]:
                allocated_resources += res_utilization["resource_allocation"]
                available_resources += res_utilization["resource_availability"]

        return (allocated_resources * 1.0) / (allocated_resources + available_resources)

    def metrics(self):
        utilz = self.__percent_utilization()
        return dict(utilization = utilz)
