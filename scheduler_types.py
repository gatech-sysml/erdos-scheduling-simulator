from enum import Enum

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