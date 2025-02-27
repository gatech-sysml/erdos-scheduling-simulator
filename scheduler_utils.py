from enum import Enum
from utils import EventTime

class SchedulerType(Enum):
    FIFO = 1
    EDF = 2
    LSF = 3
    Z3 = 4
    BranchPrediction = 5
    ILP = 6
    TetriSched_CPLEX = 7
    TetriSched_Gurobi = 8
    Clockwork = 9
    TetriSched = 10
    GraphenePrime = 11
    Graphene = 12

def instantiate_scheduler(scheduler_type: SchedulerType, args):
    flags = args["flags"]
    logger = args["logger"]
    branch_prediction_policy = args["branch_prediction_policy"]

    if scheduler_type == SchedulerType.FIFO:
        from schedulers import FIFOScheduler

        return FIFOScheduler(
            preemptive=flags.preemption,
            runtime=EventTime(flags.scheduler_runtime, EventTime.Unit.US),
            _flags=flags,
        )

    elif scheduler_type == SchedulerType.EDF:
        from schedulers import EDFScheduler

        return EDFScheduler(
            preemptive=flags.preemption,
            runtime=EventTime(flags.scheduler_runtime, EventTime.Unit.US),
            enforce_deadlines=flags.enforce_deadlines,
            _flags=flags,
        )
    
    elif scheduler_type == SchedulerType.LSF:
        from schedulers import LSFScheduler

        return LSFScheduler(
            preemptive=flags.preemption,
            runtime=EventTime(flags.scheduler_runtime, EventTime.Unit.US),
            _flags=flags,
        )
    
    elif scheduler_type == SchedulerType.Z3:
        from schedulers import Z3Scheduler

        return Z3Scheduler(
            preemptive=flags.preemption,
            runtime=EventTime(flags.scheduler_runtime, EventTime.Unit.US),
            lookahead=EventTime(flags.scheduler_lookahead, EventTime.Unit.US),
            enforce_deadlines=flags.enforce_deadlines,
            policy=branch_prediction_policy,
            branch_prediction_accuracy=flags.branch_prediction_accuracy,
            retract_schedules=flags.retract_schedules,
            release_taskgraphs=flags.release_taskgraphs,
            goal=flags.ilp_goal,
            _flags=flags,
        )
    
    elif scheduler_type == SchedulerType.BranchPrediction:
        from schedulers import BranchPredictionScheduler

        return BranchPredictionScheduler(
            preemptive=flags.preemption,
            runtime=EventTime(flags.scheduler_runtime, EventTime.Unit.US),
            policy=branch_prediction_policy,
            branch_prediction_accuracy=flags.branch_prediction_accuracy,
            release_taskgraphs=flags.release_taskgraphs,
            _flags=flags,
        )
    
    elif scheduler_type == SchedulerType.ILP:
        from schedulers import ILPScheduler

        return ILPScheduler(
            preemptive=flags.preemption,
            runtime=EventTime(flags.scheduler_runtime, EventTime.Unit.US),
            lookahead=EventTime(flags.scheduler_lookahead, EventTime.Unit.US),
            enforce_deadlines=flags.enforce_deadlines,
            policy=branch_prediction_policy,
            branch_prediction_accuracy=flags.branch_prediction_accuracy,
            retract_schedules=flags.retract_schedules,
            release_taskgraphs=flags.release_taskgraphs,
            goal=flags.ilp_goal,
            batching=flags.scheduler_enable_batching,
            time_limit=EventTime(flags.scheduler_time_limit, EventTime.Unit.S),
            log_to_file=flags.scheduler_log_to_file,
            _flags=flags,
        )
    
    elif scheduler_type == SchedulerType.TetriSched_CPLEX:
        from schedulers import TetriSchedCPLEXScheduler

        return TetriSchedCPLEXScheduler(
            preemptive=flags.preemption,
            runtime=EventTime(flags.scheduler_runtime, EventTime.Unit.US),
            lookahead=EventTime(flags.scheduler_lookahead, EventTime.Unit.US),
            enforce_deadlines=flags.enforce_deadlines,
            retract_schedules=flags.retract_schedules,
            goal=flags.ilp_goal,
            batching=flags.scheduler_enable_batching,
            time_limit=EventTime(flags.scheduler_time_limit, EventTime.Unit.S),
            time_discretization=EventTime(
                flags.scheduler_time_discretization, EventTime.Unit.US
            ),
            plan_ahead=EventTime(flags.scheduler_plan_ahead, EventTime.Unit.US),
            log_to_file=flags.scheduler_log_to_file,
            _flags=flags,
        )
    
    elif scheduler_type == SchedulerType.TetriSched_Gurobi:
        from schedulers import TetriSchedGurobiScheduler

        return TetriSchedGurobiScheduler(
            preemptive=flags.preemption,
            runtime=EventTime(flags.scheduler_runtime, EventTime.Unit.US),
            lookahead=EventTime(flags.scheduler_lookahead, EventTime.Unit.US),
            enforce_deadlines=flags.enforce_deadlines,
            retract_schedules=flags.retract_schedules,
            release_taskgraphs=flags.release_taskgraphs,
            goal=flags.ilp_goal,
            batching=flags.scheduler_enable_batching,
            time_limit=EventTime(flags.scheduler_time_limit, EventTime.Unit.S),
            time_discretization=EventTime(
                flags.scheduler_time_discretization, EventTime.Unit.US
            ),
            plan_ahead=EventTime(flags.scheduler_plan_ahead, EventTime.Unit.US),
            log_to_file=flags.scheduler_log_to_file,
            _flags=flags,
        )
    
    elif scheduler_type == SchedulerType.Clockwork:
        from schedulers import ClockworkScheduler

        return ClockworkScheduler(
            runtime=EventTime(flags.scheduler_runtime, EventTime.Unit.US),
            goal=flags.clockwork_goal,
            _flags=flags,
        )
    
    elif scheduler_type == SchedulerType.TetriSched:
        from schedulers import TetriSchedScheduler

        finer_discretization = flags.finer_discretization_at_prev_solution
        return TetriSchedScheduler(
            preemptive=flags.preemption,
            runtime=EventTime(flags.scheduler_runtime, EventTime.Unit.US),
            lookahead=EventTime(flags.scheduler_lookahead, EventTime.Unit.US),
            enforce_deadlines=flags.enforce_deadlines,
            retract_schedules=flags.retract_schedules,
            release_taskgraphs=flags.release_taskgraphs,
            goal=flags.ilp_goal,
            time_discretization=EventTime(
                flags.scheduler_time_discretization, EventTime.Unit.US
            ),
            plan_ahead=EventTime(flags.scheduler_plan_ahead, EventTime.Unit.US),
            log_to_file=flags.scheduler_log_to_file,
            adaptive_discretization=flags.scheduler_adaptive_discretization,
            _flags=flags,
            max_time_discretization=EventTime(
                flags.scheduler_max_time_discretization, EventTime.Unit.US
            ),
            max_occupancy_threshold=flags.scheduler_max_occupancy_threshold,
            finer_discretization_at_prev_solution=finer_discretization,
            finer_discretization_window=EventTime(
                flags.finer_discretization_window, EventTime.Unit.US
            ),
            plan_ahead_no_consideration_gap=EventTime(
                flags.scheduler_plan_ahead_no_consideration_gap, EventTime.Unit.US
            ),
        )
    
    elif scheduler_type == SchedulerType.GraphenePrime:
        try:
            from schedulers import TetriSchedScheduler

            return TetriSchedScheduler(
                preemptive=flags.preemption,
                runtime=EventTime(flags.scheduler_runtime, EventTime.Unit.US),
                lookahead=EventTime(flags.scheduler_lookahead, EventTime.Unit.US),
                # Graphene does not have a notion of deadlines.
                enforce_deadlines=False,
                retract_schedules=flags.retract_schedules,
                # Graphene is a DAG-aware scheduler.
                release_taskgraphs=True,
                # Graphene is a min-makespan scheduler.
                goal="min_placement_delay",
                time_discretization=EventTime(
                    flags.scheduler_time_discretization, EventTime.Unit.US
                ),
                plan_ahead=EventTime(flags.scheduler_plan_ahead, EventTime.Unit.US),
                log_to_file=flags.scheduler_log_to_file,
                _flags=flags,
            )
        except ImportError:
            logger.error(
                "Unable to import TetriSchedScheduler. "
                "Make sure you've compiled the TetriSched C++ backend."
            )
            raise RuntimeError("TetriSchedScheduler not available.")
        
    elif scheduler_type == SchedulerType.Graphene:
        try:
            from schedulers import GrapheneScheduler

            return GrapheneScheduler(
                preemptive=flags.preemption,
                runtime=EventTime(flags.scheduler_runtime, EventTime.Unit.US),
                lookahead=EventTime(flags.scheduler_lookahead, EventTime.Unit.US),
                retract_schedules=flags.retract_schedules,
                goal=flags.ilp_goal,
                time_discretization=EventTime(
                    flags.scheduler_time_discretization, EventTime.Unit.US
                ),
                plan_ahead=EventTime(flags.scheduler_plan_ahead, EventTime.Unit.US),
                log_to_file=flags.scheduler_log_to_file,
                _flags=flags,
            )
        except ImportError:
            logger.error(
                "Unable to import GrapheneScheduler. "
                "Make sure you've compiled the TetriSched C++ backend."
            )
            raise RuntimeError("GrapheneScheduler not available.")