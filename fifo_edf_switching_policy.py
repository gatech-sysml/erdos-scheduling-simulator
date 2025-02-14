from scheduler_types import SchedulerType
from scheduler_switching_policy import SchedulerSwitchingPolicy

class FifoEdfSwitchingPolicy(SchedulerSwitchingPolicy):
    def pick_scheduler(self, metrics, profiles=None, initial_invocation=True) -> SchedulerType:
        """
        Returns the name of the next scheduler to switch to.

        Args:
            metrics: System metrics like utilization, current SLO attainment etc. 
            profiles: (optional) Running profiles of scheduling algorithms for un-opinionated switching

        Returns:
            SchedulerType: Enum representing next scheduler 
        """
        if(initial_invocation):
            return SchedulerType.FIFO
        
        if(metrics["utilization"] <= 0.25 or (metrics["utilization"] > 0.5 and metrics["utilization"] < 0.75)):
            return SchedulerType.FIFO
        
        else:
            return SchedulerType.EDF
            