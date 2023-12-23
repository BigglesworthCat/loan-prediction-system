from enum import Enum


class HomeOwnership(Enum):
    RENT = 'Rent'
    OWN = 'Own'
    MORTGAGE = 'Mortgage'
    OTHER = 'Other'


class LoanIntent(Enum):
    PERSONAL = 'Personal'
    EDUCATION = 'Education'
    MEDICAL = 'Medical'
    VENTURE = 'Venture'
    HOME_IMPROVEMENT = 'Home improvement'
    DEBT_CONSOLIDATION = 'Debt consolidation'


class PreviousDefault(Enum):
    YES = 'Yes'
    NO = 'No'


class LoanGrade(Enum):
    A = 'A'
    B = 'B'
    C = 'C'
    D = 'D'
    E = 'E'
    F = 'F'
    G = 'G'


class PredictionModel(Enum):
    LOGISTIC_REGRESSION = 'Logistic regression'
    DECISION_TREE = 'Decision tree'
    RANDOM_FOREST = 'Random forest'
    GRADIENT_BOOSTING_MACHINES = 'Gradient boosting machines'
