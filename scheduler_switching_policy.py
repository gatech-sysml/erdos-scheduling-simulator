from abc import ABC, abstractmethod
from scheduler_types import SchedulerType

class SchedulerSwitchingPolicy(ABC):
    """Interface for defining a scheduler switching policy."""

    @abstractmethod
    def pick_scheduler(self, metrics, profiles=None, initial_invocation=True) -> SchedulerType:
        """
        Returns the name of the next scheduler to switch to.

        Args:
            metrics: System metrics like utilization, current SLO attainment etc. 
            profiles: (optional) Running profiles of scheduling algorithms for un-opinionated switching

        Returns:
            SchedulerType: Enum representing next scheduler 
        """
        pass
