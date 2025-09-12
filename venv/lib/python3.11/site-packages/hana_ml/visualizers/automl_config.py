"""
This module contains related class for more convenient writing automl config dict.

The following class is available:

    * :class:`AutoMLConfig`
"""

# pylint: disable=missing-function-docstring
# pylint: disable=invalid-name
# pylint: disable=undefined-variable
# pylint: disable=no-else-break
# pylint: disable=unidiomatic-typecheck
# pylint: disable=bare-except
# pylint: disable=missing-class-docstring
import os
import json
import time
from typing import Union
from urllib.parse import quote
from hana_ml.visualizers.shared import EmbeddedUI


PARAMETER_METADATA = {
    "LabelEncoder": {
        "IGNORE_UNKNOWN": {
            "description": [
                "0: Categorical features in predict data must not contain unknown categorical value",
                "1: Unknown categorical values will be encoded from -1 to -n, n is number of unknown values"
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "OneHotEncoder": {
        "IGNORE_UNKNOWN": {
            "description": [
                "0: Categorical features in predict data must not contain unknown categorical value",
                "1: Unknown categorical values will be encoded from -1 to -n, n is number of unknown values"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MINIMUM_FRACTION": {
            "description": [
                "Minimum fraction of unique values in a feature to consider the feature to be categorical."
            ],
            "type": [
                "DOUBLE"
            ]
        }
    },
    "Imputer": {
        "IMPUTATION_TYPE": {
            "description": [
                "Chooses the overall default imputation type for all columns:",
                "0: NONE,",
                "1: MODE_MEAN,",
                "2: MODE_MEDIAN,",
                "3: MODE_ALLZERO,",
                "4: MODE_ALS,",
                "5: DELETE.",
                "NONE and DELETE: set all columns to imputation type NONE/DELETE.",
                "MODE_MEAN, MODE_MEDIAN, MODE_ALLZERO, and MODE_ALS: set categorical columns to the first imputation type (the type before “_”); set numerical columns to the second imputation type (the type after “_”). ALLZERO means to fill all missing values in numerical columns with 0."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "*<column name>*_IMPUTATION_TYPE": {
            "description": [
                "Sets imputation type of a specified column. This value overwrites the default imputation type deduced by the IMPUTATION_TYPE parameter:",
                "0: NONE,",
                "1: DELETE,",
                "100: MODE,",
                "101: SPECIFIED_CATEGORICAL_VALUE,",
                "200: MEAN,",
                "201: MEDIAN,",
                "203: SPECIFIED_NUMERICAL_VALUE,",
                "204: ALS.",
                "For type 101 (SPECIFIED_CATEGORICAL_VALUE), use the following syntax to specify a categorical value to be filled in a column V0:",
                "INSERT INTO #PAL_PARAMETER_TBL VALUES(‘V0_IMPUTATION_TYPE’, 101, NULL, ‘<CATEGORICAL_VALUE>’);",
                "For type 203 (SPECIFIED_NUMERICAL_VALUE), use the following syntax to specify a numerical value to be filled in a column V1:",
                "INSERT INTO #PAL_PARAMETER_TBL VALUES(‘V1_IMPUTATION_TYPE’, 203, <NUMERICAL_VALUE>, NULL);",
                "Note: A column of integer type can be set to type 101 (SPECIFIED_CATEGORICAL_VALUE) or type 203 (SPECIFIED_NUMERICAL_VALUE):",
                "when setting to type 101 (SPECIFIED_CATEGORICAL_VALUE), the string must contain a valid integer value, otherwise an error will be thrown;",
                "when setting to type 203 (SPECIFIED_NUMERICAL_VALUE), the double value will be converted to integer."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ALS_FACTOR_NUMBER": {
            "description": [
                "Length of factor vectors, that is, the `f` in ALS model.",
                "Note: ALS_FACTOR_NUMBER should be less than the number of numerical columns to get a meaningful result."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ALS_REGULARIZATION": {
            "description": [
                "L2 regularization of the factors, that is, the `λ` in ALS model."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "ALS_MAX_ITERATION": {
            "description": [
                "Maximum number of iterations when training ALS model."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ALS_SEED": {
            "description": [
                "Specifies the seed of the random number generator used in the training of ALS model:",
                "0: Uses the current time as the seed,",
                "Others: Uses the specified value as the seed."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ALS_EXIT_THRESHOLD": {
            "description": [
                "The training of ALS model exits if the value of cost function is not decreased more than the value of ALS_EXIT_THRESHOLD since the last check. If ALS_EXIT_THRESHOLD is set to 0, there is no check and the training only exits on reaching the maximum number of iterations. Evaluations of cost function require additional calculations. You can set this parameter to 0 to avoid it."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "ALS_EXIT_INTERVAL": {
            "description": [
                "During the training of ALS model, it will calculate cost function and check the exit criteria every <ALS_EXIT_INTERVAL> iterations. Evaluations of cost function require additional calculations."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ALS_LINEAR_SYSTEM_SOLVER": {
            "description": [
                "0: cholesky solver",
                "1: cg solver (recommended when ALS_FACTOR_NUMBER is large)"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ALS_CG_MAX_ITERATION": {
            "description": [
                "Maximum number of iteration of cg solver."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ALS_CENTERING": {
            "description": [
                "Whether to center the data by column before training ALS model.",
                "0: no centering,",
                "1: centering."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ALS_SCALING": {
            "description": [
                "Whether to scale the data by column before training ALS model.",
                "0: no scaling,",
                "1: scaling."
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "PolynomialFeatures": {
        "MIN_DEGREE": {
            "description": [
                "The minimum polynomial degree of generated features."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_DEGREE": {
            "description": [
                "The maximum polynomial degree of generated features."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "INTERACTION_ONLY": {
            "description": [
                "When set to True, only interaction features are produced."
            ],
            "type": [
                "BOOL"
            ]
        },
        "INCLUDE_BIAS": {
            "description": [
                "When set to True, a bias column is included in the output table. For features where all polynomial powers are zero, set the value of this bias column to 1."
            ],
            "type": [
                "BOOL"
            ]
        }
    },
    "CATPCA": {
        "COMPONENTS_PERCENTAGE": {
            "description": [
                "The percentage of columns in the data to keep."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "SCALING": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies whether variable should be scaled to unit variance before analysis.",
                "Note: when data instance contains category variable, this parameter will always be treated as 1.",
                "Valid value:",
                "0: No",
                "1: Yes"
            ]
        },
        "SCORES": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies whether to output component scores.",
                "Valid value",
                "0: No",
                "1: Yes"
            ]
        },
        "COMPONENT_TOL": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Specifies the threshold of the ratio of current singular value to the largest singular value whose component will be dropped in final output. If not specified, no component will be dropped. Valid range is [0, 1]."
            ]
        },
        "MAX_ITERATION": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies the maximum iteration count for optimal scaling quantification. Valid range is [1,∞]."
            ]
        },
        "CONVERGE_TOL": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Specifies the convergence criteria to detect convergence for optimal scaling quantification. Valid range is (0,1)."
            ]
        },
        "LANCZOS_ITERATION": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies maximum iteration when using LANCZOS algorithm to compute SVD. Valid only when SVD_CALCULATOR is 0. Valid range is [1,∞]."
            ]
        },
        "SVD_CALCULATOR": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies the algorithm to calculate SVD.",
                "Note: When N_COMPONENTS equals to data dimension, this parameter will always be 1.",
                "Valid Values:",
                "0: LANCZOS Algorithm",
                "1: Jacobi Algorithm"
            ]
        }
    },
    "FS_supervised": {
        "FS_METHOD": {
            "description": [
                "Feature selection method.",
                "0: Anova",
                "1: Chi-squared",
                "2: Gini Index",
                "3: Fisher Score",
                "4: Information Gain",
                "5: Minimum Redundancy Maximum Relevance (MRMR)",
                "6: Joint Mutual Information (JMI)",
                "7: Interaction Weight Based Feature Selection (IWFS)",
                "8: Fast Correlation Based Filter (FCBF)",
                "11: ReliefF",
                "12: ADMM",
                "13: Competitive Swarm Optimizer (CSO)"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "TOP_K_BEST": {
            "description": [
                "Top k features to be selected",
                "Must be assigned a value except for FCBF(8) and CSO(13)"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "FS_THRESHOLD": {
            "description": [
                "Predefined threshold for SU values between features and target.",
                "Only Valid when FS_METHOD is FCBF(8)."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "FS_ROWSAMPLING_RATIO": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The ratio of random sampling without replacement."
            ]
        },
        "FS_N_NEIGHBOURS": {
            "description": [
                "Number of neighbors considered in the computation of affinity matrix",
                "Only Valid when FS_METHOD is 9, 10, 11."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "FS_CATEGORY_WEIGHT": {
            "description": [
                "The weight of categorical features whilst calculating distance.",
                "Only Valid when FS_METHOD is 9, 10, 11."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "FS_SIGMA": {
            "description": [
                "`σ` in above equation.",
                "Only Valid when FS_METHOD is 9, 10, 11."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "FS_MAX_ITER": {
            "description": [
                "Maximal iterations allowed to run optimization.",
                "Only Valid when FS_METHOD is ADMM(12)."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "FS_ADMM_TOL": {
            "description": [
                "Convergence threshold.",
                "Only Valid when FS_METHOD is ADMM(12)."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "FS_ADMM_RHO": {
            "description": [
                "Penalty factor.",
                "Only Valid when FS_METHOD is ADMM(12)."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "FS_ADMM_MU": {
            "description": [
                "Gain of `FS_ADMM_RHO` at each iteration.",
                "Only Valid when FS_METHOD is ADMM(12)."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "FS_ADMM_GAMMA": {
            "description": [
                "Regularization coefficient.",
                "Only Valid when FS_METHOD is ADMM(12)."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "CSO_REPEAT_NUM": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Number of repetitions to run `CSO`. `CSO` starts with a different initialization at each time.",
                "Only Valid when FS_METHOD is CSO(13)."
            ]
        },
        "CSO_MAXGENERATION_NUM": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Maximal number of generations.",
                "Only Valid when FS_METHOD is CSO(13)."
            ]
        },
        "CSO_EARLYSTOP_NUM": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Stop if there's no change in `CSO_EARLYSTOP_NUM` generation.",
                "Only Valid when FS_METHOD is CSO(13)."
            ]
        },
        "CSO_POPULATION_SIZE": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Population size of the swarm particles.",
                "Only Valid when FS_METHOD is CSO(13)."
            ]
        },
        "CSO_PHI": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Social factor.",
                "Only Valid when FS_METHOD is CSO(13)."
            ]
        },
        "CSO_FEATURENUM_PENALTY": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Penalize the number of features.",
                "Only Valid when FS_METHOD is CSO(13)."
            ]
        },
        "CSO_TEST_RATIO": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The ratio for the splitting of training data and testing data.",
                "Only Valid when FS_METHOD is CSO(13)."
            ]
        }
    },
    "FS_unsupervised": {
        "FS_METHOD": {
            "description": [
                "Feature selection method.",
                "9: Laplacian Score",
                "10: Spectral Feature Selection (SPEC)"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "TOP_K_BEST": {
            "description": [
                "Top k features to be selected."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "FS_REGULARIZATION_POWER": {
            "description": [
                "The order of the power function that penalizes high frequency components.",
                "Only Valid when FS_METHOD is SPEC(10)."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "FS_ROWSAMPLING_RATIO": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The ratio of random sampling without replacement."
            ]
        },
        "FS_N_NEIGHBOURS": {
            "description": [
                "Number of neighbors considered in the computation of affinity matrix",
                "Only Valid when FS_METHOD is 9, 10, 11."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "FS_CATEGORY_WEIGHT": {
            "description": [
                "The weight of categorical features whilst calculating distance.",
                "Only Valid when FS_METHOD is 9, 10, 11."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "FS_SIGMA": {
            "description": [
                "`σ` in above equation.",
                "Only Valid when FS_METHOD is 9, 10, 11."
            ],
            "type": [
                "DOUBLE"
            ]
        }
    },
    "SCALE": {
        "NEW_RANGE": {
            "description": [
                "Defines the range of the new data after scaling. the maximun value is the sum of the minimum value of min-max normalization method and NEW_RANGE."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "SCALING_METHOD": {
            "description": [
                "Scaling method:",
                "0: Min-max normalization",
                "1: Z-Score normalization",
                "2: Decimal scaling normalization"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "Z-SCORE_METHOD": {
            "description": [
                "0: Mean-Standard deviation",
                "1: Mean-Mean absolute deviation",
                "2: Median-Median absolute deviation"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "NEW_MIN": {
            "description": [
                "The new minimum value of min-max normalization method"
            ],
            "type": [
                "DOUBLE"
            ]
        }
    },
    "SAMPLING": {
        "PERCENTAGE": {
            "description": [
                "Specify the percentage of input data to be sampled."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "SAMPLING_METHOD": {
            "description": [
                "Sampling method:",
                "0: First_N",
                "1: Middle_N",
                "2: Last_N",
                "3: Every_Nth",
                "4: SimpleRandom_WithReplacement",
                "5: SimpleRandom_WithoutReplacement",
                "6: Systematic",
                "7: Stratified_WithReplacement",
                "8: Stratified_WithoutReplacement",
                "Note: For the random methods (method 4, 5, 6 in the above list), the system time is used for the seed."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "INTERVAL": {
            "description": [
                "The interval between two samples."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "COLUMN_CHOOSE": {
            "description": [
                "The column that is used to do the stratified sampling."
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "SMOTE": {
        "K_NEAREST_NEIGHBOURS": {
            "description": [
                "Number of nearest neighbors (k)."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MINORITY_CLASS": {
            "description": [
                "Specifies which minority class value in dependent variable column will perform SMOTE."
            ],
            "type": [
                "NVARCHAR"
            ]
        },
        "SMOTE_AMOUNT": {
            "description": [
                "Amount of SMOTE, N%. For example, 200 means 200%, which means each minority class sample will generate two synthetic samples."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "VARIABLE_WEIGHT": {
            "description": [
                "Specifies the weight of a variable participating in distance calculation. The value must be greater or equal to 0. Defaults to 1 for variables not specified."
            ],
            "type": [
                "NVARCHAR for variable name and DOUBLE for weight value"
            ]
        },
        "CATEGORY_WEIGHTS": {
            "description": [
                "Represents the weight of category attributes. The value must be greater or equal to 0."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "METHOD": {
            "description": [
                "Searching method when finding K nearest neighbor.",
                "0: Brute force searching",
                "1: KD-tree searching"
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "SMOTETomek": {
        "K_NEAREST_NEIGHBOURS": {
            "description": [
                "Number of nearest neighbors (k)."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MINORITY_CLASS": {
            "description": [
                "Specifies which minority class value in dependent variable column will perform SMOTE."
            ],
            "type": [
                "NVARCHAR"
            ]
        },
        "SMOTE_AMOUNT": {
            "description": [
                "Amount of SMOTE, N%. For example, 200 means 200%, which means each minority class sample will generate two synthetic samples."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "VARIABLE_WEIGHT": {
            "description": [
                "Specifies the weight of a variable participating in distance calculation. The value must be greater or equal to 0. Defaults to 1 for variables not specified."
            ],
            "type": [
                "NVARCHAR for variable name and DOUBLE for weight value"
            ]
        },
        "CATEGORY_WEIGHTS": {
            "description": [
                "Represents the weight of category attributes. The value must be greater or equal to 0."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "METHOD": {
            "description": [
                "Searching method when finding K nearest neighbor.",
                "0: Brute force searching",
                "1: KD-tree searching"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "SAMPLING_STRATEGY": {
            "description": [
                "Specify the class targeted by the resampling.",
                "0: Resamples only the majority class.",
                "1: Resamples all classes but the minority class.",
                "2: Resamples all classes but the majority class.",
                "3: Resamples all classes."
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "TomekLinks": {
        "SAMPLING_STRATEGY": {
            "description": [
                "Specify the class targeted by the resampling.",
                "0: Resamples only the majority class.",
                "1: Resamples all classes but the minority class.",
                "2: Resamples all classes but the majority class.",
                "3: Resamples all classes."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "VARIABLE_WEIGHT": {
            "description": [
                "Specifies the weight of a variable participating in distance calculation. The value must be greater or equal to 0. Defaults to 1 for variables not specified."
            ],
            "type": [
                "NVARCHAR for variable name and DOUBLE for weight value"
            ]
        },
        "CATEGORY_WEIGHTS": {
            "description": [
                "Represents the weight of category attributes. The value must be greater or equal to 0."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "DISTANCE_LEVEL": {
            "description": [
                "Computes the distance between the train data and the test data point.",
                "1: Manhattan distance",
                "2: Euclidean distance",
                "3: Minkowski distance",
                "4: Chebyshev distance",
                "6: Cosine distance"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MINKOWSKI_POWER": {
            "description": [
                "When you use the Minkowski distance, this parameter controls the value of power."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "METHOD": {
            "description": [
                "Searching method when finding K nearest neighbor.",
                "0: Brute force searching",
                "1: KD-tree searching"
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "HGBT_Classifier": {
        "SPLIT_METHOD": {
            "description": [
                "The method to find split point for numeric feature.",
                "'exact': the exact method, trying all possible points",
                "'sketch': the sketch method, accounting for the distribution of the sum of hessian",
                "'sampling': samples the split point randomly",
                "'histogram': builds histogram upon data and uses it as split point"
            ],
            "type": [
                "VARCHAR",
                "NVARCHAR"
            ]
        },
        "SKETCH_EPS": {
            "description": [
                "The epsilon of the sketch method. It indicates that the sum of hessian between two split points is not larger than this value. That is, the number of bins is approximately 1/eps."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "MAX_BIN_NUM": {
            "description": [
                "The maximum bin number for histogram method. Decreasing this number gains better performance in terms of running time at a cost of accuracy."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ITER_NUM": {
            "description": [
                "Total iteration number, which is equivalent to the number of trees in the final model."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_DEPTH": {
            "description": [
                "The maximum depth of each tree."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "NODE_SIZE": {
            "description": [
                "The minimum number of data in a leaf."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ETA": {
            "description": [
                "Learning rate of each iteration.",
                "Range: (0, 1)"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "GAMMA": {
            "description": [
                "The minimum loss change value to make a split in tree growth (gamma in the equation)."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "MIN_CHILD_HESSIAN": {
            "description": [
                "The minimum summation of sample weights (hessian) in the leaf node."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "NODE_WEIGHT_CONSTRAINT": {
            "description": [
                "The maximum weight constraint assigned to each tree node. 0 for no constraint."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "ROW_SAMPLE_RATE": {
            "description": [
                "The sample rate of row (data points).",
                "Range: (0, 1)"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "COL_SAMPLE_RATE_BYSPLIT": {
            "description": [
                "The sample rate of feature set in each split.",
                "Range: (0, 1)"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "COL_SAMPLE_RATE_BYTREE": {
            "description": [
                "The sample rate of feature set in each tree growth.",
                "Range: (0, 1)"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "LAMBDA": {
            "description": [
                "L2 regularization.",
                "Range: [0, 1]"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "ALPHA": {
            "description": [
                "L1 regularization.",
                "Range: [0, 1]"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "BASE_SCORE": {
            "description": [
                "Initial prediction score of all instances.",
                "Global bias for sufficient number of iterations.",
                "Range: (0, 1), only for binary classification."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "START_FROM_AVERAGE": {
            "description": [
                "Indicates whether to adopt the prior distribution as the initial point. To be specific, use average value if it is a regression problem, and use frequencies of labels if it is a classification problem.",
                "0: no",
                "1: yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "OBJ_FUNC": {
            "description": [
                "For binary classification:",
                "5: Logistic",
                "6: Hinge",
                "For multiple classification:",
                "7: Softmax"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "TWEEDIE_POWER": {
            "description": [
                "For Tweedie object function; must in range [1.0, 2.0]"
            ],
            "type": [
                "Double"
            ]
        },
        "HUBER_SLOPE": {
            "description": [
                "For Huber/Pseudo Huber object function; must be greater than 0."
            ],
            "type": [
                "Double"
            ]
        },
        "REPLACE_MISSING": {
            "description": [
                "Replace missing value by another value in the feature. If it is a continuous feature, the value is the mean value; if it is a categorical feature, the value is the most frequent one.",
                "0: no",
                "1: yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "DEFAULT_MISSING_DIRECTION": {
            "description": [
                "Define the default direction where missing value will go to while splitting.",
                "0: left",
                "1: right"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "FEATURE_GROUPING": {
            "description": [
                "Apply feature grouping by grouping sparse features that only contains one significant value in each row.",
                "0: no",
                "1: yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "TOLERANT_RATE": {
            "description": [
                "While applying feature grouping, still merge features when there are rows containing more than one significant value. This parameter specifies the rate of such rows allowed."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "FG_MIN_ZERO_RATE": {
            "description": [
                "Specifies the minimum zero rate that is used to indicate sparse columns, only which may take part in feature grouping."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "CALCULATE_IMPORTANCE": {
            "description": [
                "Determines whether to calculate variable importance:",
                "0: no",
                "1: yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "CALCULATE_CONFUSION_MATRIX": {
            "description": [
                "Determines whether to calculate confusion matrix:",
                "0: no",
                "1: yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "COMPRESSION": {
            "description": [
                "Indicate whether result model should be compressed:",
                "0: no",
                "1: yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_BITS": {
            "description": [
                "Maximal number of bits to quantize continuous features. Equivalent to use 2<sup>MAX_BITS</sup> bins."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "VALIDATION_SET_RATE": {
            "description": [
                "Specifies the rate of validation set that be sampled from data set. If 0.0 is set, then no early stop will be applied."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "STRATIFIED_VALIDATION_SET": {
            "description": [
                "Indicates whether to apply stratified method while sampling validation set."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "TOLERANT_ITER_NUM": {
            "description": [
                "Indicates how many consecutive deteriorated iterations should be observed before applying early stop."
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "MLP_Classifier": {
        "ACTIVATION": {
            "description": [
                "Active function code for the hidden layer.",
                "TANH = 1",
                "LINEAR = 2",
                "SIGMOID_ASYMMETRIC = 3",
                "SIGMOID_SYMMETRIC = 4",
                "GAUSSIAN_ASYMMETRIC = 5",
                "GAUSSIAN_SYMMETRIC = 6",
                "ELLIOT_ASYMMETRIC = 7",
                "ELLIOT_SYMMETRIC = 8",
                "SIN_ASYMMETRIC = 9",
                "SIN_SYMMETRIC = 10",
                "COS_ASYMMETRIC = 11",
                "COS_SYMMETRIC = 12",
                "ReLU = 13"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "OUTPUT_ACTIVATION": {
            "description": [
                "Active function code for the output layer. The code is the same as ACTIVATION."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "HIDDEN_LAYER_SIZE": {
            "description": [
                "Specifies the size of each hidden layer in the format of '2, 3, 4'. The value 0 will be ignored, for example, '2, 0, 3' is equal to '2, 3'."
            ],
            "type": [
                "NVARCHAR"
            ]
        },
        "MAX_ITER": {
            "description": [
                "Maximum number of iterations."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "TRAINING_STYLE": {
            "description": [
                "Specifies the training style:",
                "0: Batch",
                "1:Stochastic"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "LEARNING_RATE": {
            "description": [
                "Specifies the learning rate."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "MOMENTUM": {
            "description": [
                "Specifies the momentum factor."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "BATCH_SIZE": {
            "description": [
                "Specifies the size of mini batch."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "NORMALIZATION": {
            "description": [
                "Specifies the normalization type:",
                "0: None",
                "1: Z-transform",
                "2: Scalar"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "WEIGHT_INIT": {
            "description": [
                "Specifies the weight initial value:",
                "0: All zeros",
                "1: Normal distribution",
                "2: Uniform distribution in range (0, 1)",
                "3: Variance scale normal",
                "4: Variance scale uniform"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ONEHOT_MIN_FREQUENCY": {
            "description": [
                "Specifies the minimum frequency below which a category is considered infrequent."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ONEHOT_MAX_CATEGORIES": {
            "description": [
                "Specifies an upper limit of the number of output features for each input feature. ONEHOT_MAX_CATEGORIES includes the feature that combines infrequent categories."
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "RDT_Classifier": {
        "TREES_NUM": {
            "description": [
                "Specifies the number of trees to grow."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "TRY_NUM": {
            "description": [
                "Specifies the number of randomly selected variables for splitting."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ALLOW_MISSING_DEPENDENT": {
            "description": [
                "Specifies if missing target value is allowed.",
                "0: Not allowed. An error occurs if missing dependent presents.",
                "1: Allowed. The datum with missing dependent is removed."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "NODE_SIZE": {
            "description": [
                "Specifies the minimum number of records in a leaf."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "SPLIT_THRESHOLD": {
            "description": [
                "Specifies the stop condition: if the improvement value of the best split is less than this value, the tree stops growing."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "CALCULATE_OOB": {
            "description": [
                "Indicates whether to calculate out-of-bag error.",
                "0: No",
                "1: Yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_DEPTH": {
            "description": [
                "The maximum depth of a tree."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "SAMPLE_FRACTION": {
            "description": [
                "The fraction of data used for training. Assume there are `n` pieces of data, SAMPLE_FRACTION is `r`, then `n*r` data is selected for training."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "STRATA": {
            "description": [
                "Specifies the stratified sampling for classification. This is a pair parameter, which means a parameter name corresponds to two values. The first value is the class label (INTEGER or VARCHAR/NVARCHAR), and the second one is the proportion of this class occupies in the sampling data. The class with a proportion less than 0 or larger than 1 will be ignored. If some classes are absent, they share the remaining proportion equally. If the summation of strata is less than 1, the remaining is shared by all classes equally."
            ],
            "type": [
                "(INTEGER, DOUBLE)",
                "(VARCHAR/NVARCHAR, DOUBLE)"
            ]
        },
        "PRIORS": {
            "description": [
                "Specifies the prior probabilities for classification. This is also a pair parameter. The first value is the class label (INTEGER or VARCHAR/NVARCHAR), and the second one is the prior probability of this class. The class with a prior probability less than 0 or larger than 1 will be ignored. If some classes are absent, they share the remaining probability equally. If the summation of priors is less than 1, the remaining is shared by all classes equally."
            ],
            "type": [
                "(INTEGER, DOUBLE)",
                "(VARCHAR/NVARCHAR, DOUBLE)"
            ]
        },
        "COMPRESSION": {
            "description": [
                "Specifies if the model is stored in compressed format",
                "0: No",
                "1: Yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_BITS": {
            "description": [
                "The maximum number of bits to quantize continuous features. Equivalent to use 2<sup>MAX_BITS</sup> bins to quantize the values of these continuous features. Reduce the size of bins may affect the precision of split values and the accuracy in prediction. Must be less than 31."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "QUANTIZE_RATE": {
            "description": [
                "This value decides whether to do quantization for split values of a certain continuous feature. If the largest frequency of these continuous split values is less than QUANTIZE_RATE, quantization method will be used to quantize the split values of the continuous feature."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "FITTINGS_QUANTIZATION": {
            "description": [
                "This parameter indicates whether to quantize fitting values (the values of leaves) in regression problem.",
                "0: No",
                "1: Yes",
                "It is recommended to use this technique for large dataset in regression problem."
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "NB_Classifier": {
        "LAPLACE": {
            "description": [
                "Enables or disables Laplace smoothing.",
                "0: Disables Laplace smoothing",
                "Positive value: Enables Laplace smoothing for discrete values",
                "Note: The LAPLACE value is only stored by JSON format models. If the PMML format is chosen, you may need to set the LAPLACE value again in the predicting phase."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "DISCRETIZATION": {
            "description": [
                "0: Disables discretization.",
                "Other values: Uses supervised discretization to all the continuous attributes."
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "DT_Classifier": {
        "ALGORITHM": {
            "description": [
                "Specifies the algorithm used to grow a decision tree.",
                "1: C45",
                "2: CHAID",
                "3: CART"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "PERCENTAGE": {
            "description": [
                "Specifies the percentage of the input data that will be used to build the tree model.",
                "For example, if you set this parameter to 0.7, then 70% of the training data will be used to build the tree model and 30% will be used to prune the tree model."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "MIN_RECORDS_OF_PARENT": {
            "description": [
                "Specifies the stop condition: if the number of records in one node is less than the specified value, the algorithm stops splitting."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MIN_RECORDS_OF_LEAF": {
            "description": [
                "Promises the minimum number of records in a leaf."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_DEPTH": {
            "description": [
                "Specifies the stop condition: if the depth of the tree model is greater than the specified value, the algorithm stops splitting."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "SPLIT_THRESHOLD": {
            "description": [
                "Specifies the stop condition for a node:",
                "C45: The information gain ratio of the best split is less than this value.",
                "CHAID: The p-value of the best split is greater than or equal to this value.",
                "CART: The reduction of Gini index or relative MSE of the best split is less than this value.",
                "The smaller the SPLIT_THRESHOLD value is, the larger a C45 or CART tree grows. On the contrary, CHAID will grow a larger tree with larger SPLIT_THRESHOLD value."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "*<name of target value>*_PRIOR_": {
            "description": [
                "Specifies the priori probability of every class label."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "DISCRETIZATION_TYPE": {
            "description": [
                "Specifies the strategy for discretizing continuous attributes:",
                "0: MDLPC",
                "1: Equal Frequency"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "*<column name>*_BIN_": {
            "description": [
                "Specifies the number of bins for discretisation"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_BRANCH": {
            "description": [
                "Specifies the maximum number of branches."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MERGE_THRESHOLD": {
            "description": [
                "Specifies the merge condition for CHAID: if the metric value is greater than or equal to the specified value, the algorithm will merge two branches."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "USE_SURROGATE": {
            "description": [
                "Indicates whether to use surrogate split when NULL values are encountered.",
                "0: Does not use surrogate split.",
                "1: Uses surrogate split."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "IS_OUTPUT_RULES": {
            "description": [
                "Specifies whether to output decision rules or not.",
                "0: Does not output decision rules.",
                "1: Outputs decision rules."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "IS_OUTPUT_CONFUSION_MATRIX": {
            "description": [
                "Specifies whether to output confusion matrix or not.",
                "0: Does not output confusion matrix.",
                "1: Outputs confusion matrix."
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "M_LOGR_Classifier": {
        "MAX_ITERATION": {
            "description": [
                "Maximum number of iterations of the optimization."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "METHOD": {
            "description": [
                "0: lbfgs",
                "1: cyclical coordinate descent"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ENET_LAMBDA": {
            "description": [
                "Penalized weight. The value should be equal to or greater than 0."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "ENET_ALPHA": {
            "description": [
                "The elastic net mixing parameter. The value range is between 0 and 1 inclusively.",
                "0: Ridge penalty",
                "1: LASSO penalty"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "EXIT_THRESHOLD": {
            "description": [
                "Convergence threshold for exiting iterations."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "STANDARDIZE": {
            "description": [
                "Controls whether to standardize the data to have zero mean and unit variance.",
                "0: No",
                "1: Yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "STAT_INF": {
            "description": [
                "Controls whether to proceed statistical inference.",
                "0: Does not proceed statistical inference.",
                "1: Proceeds statistical inference."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ONEHOT_MIN_FREQUENCY": {
            "description": [
                "Specifies the minimum frequency below which a category is considered infrequent."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ONEHOT_MAX_CATEGORIES": {
            "description": [
                "Specifies an upper limit of the number of output features for each input feature. ONEHOT_MAX_CATEGORIES includes the feature that combines infrequent categories."
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "SVM_Classifier": {
        "KERNEL_TYPE": {
            "description": [
                "Kernel type:",
                "0: LINEAR KERNEL",
                "1: POLY KERNEL",
                "2: RBF KERNEL",
                "3: SIGMOID KERNEL"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "POLY_DEGREE": {
            "description": [
                "Coefficient for the PLOY KERNEL type.",
                "Value range: ≥ 1"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "RBF_GAMMA": {
            "description": [
                "Coefficient for the RBF KERNEL type.",
                "Value range: > 0"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "COEF_LIN": {
            "description": [
                "Coefficient for the POLY/SIGMOID KERNEL type."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "COEF_CONST": {
            "description": [
                "Coefficient for the POLY/SIGMOID KERNEL type."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "SVM_C": {
            "description": [
                "Trade-off between training error and margin.",
                "Value range: > 0"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "SCALE_INFO": {
            "description": [
                "No scale",
                "1: Standardization",
                "The algorithm transforms the data to have zero mean and unit variance. The general formula is given as:",
                "where x is the origin feature vector, μ is the mean of that feature vector, and σ is its standard deviation.",
                "2: Rescaling",
                "The algorithm rescales the range of the features to scale the range in [-1,1]. The general formula is given as:",
                "where x is the origin feature vector."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "SCALE_LABEL": {
            "description": [
                "Controls whether to standardize the label for SVR.",
                "0: No",
                "1: Yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "SHRINK": {
            "description": [
                "Decides whether to use shrink strategy or not:",
                "0: Does not use shrink strategy",
                "1: Uses shrink strategy"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "HANDLE_MISSING": {
            "description": [
                "Whether to handle missing values:",
                "0: No",
                "1: Yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "CATEGORY_WEIGHT": {
            "description": [
                "Represents the weight of category attributes (γ). The value must be greater than 0."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "TOL": {
            "description": [
                "Specifies the error tolerance in the training process. The value must be greater than 0."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "EVALUATION_SEED": {
            "description": [
                "The random seed in parameter selection. The value must be greater than 0."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "COMPRESSION": {
            "description": [
                "Specifies if the model is stored in compressed format.",
                "0 : no",
                "1: yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_BITS": {
            "description": [
                "The maximum number of bits to quantize continuous features. Equivalent to use 2^MAX_BITS bins. Must be less than 31."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_QUANTIZATION_ITER": {
            "description": [
                "The maximum iteration steps for quantization."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ONEHOT_MIN_FREQUENCY": {
            "description": [
                "Specifies the minimum frequency below which a category is considered infrequent."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ONEHOT_MAX_CATEGORIES": {
            "description": [
                "Specifies an upper limit of the number of output features for each input feature. ONEHOT_MAX_CATEGORIES includes the feature that combines infrequent categories."
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "HGBT_Regressor": {
        "SPLIT_METHOD": {
            "description": [
                "The method to find split point for numeric feature.",
                "exact: the exact method, trying all possible points",
                "sketch: the sketch method, accounting for the distribution of the sum of hessian",
                "sampling: samples the split point randomly",
                "histogram:builds histogram upon data and uses it as split point"
            ],
            "type": [
                "VARCHAR",
                "NVARCHAR"
            ]
        },
        "SKETCH_EPS": {
            "description": [
                "The epsilon of the sketch method. It indicates that the sum of hessian between two split points is not larger than this value. That is, the number of bins is approximately 1/eps."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "MAX_BIN_NUM": {
            "description": [
                "The maximum bin number for histogram method. Decreasing this number gains better performance in terms of running time at a cost of accuracy."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ITER_NUM": {
            "description": [
                "Total iteration number, which is equivalent to the number of trees in the final model."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_DEPTH": {
            "description": [
                "The maximum depth of each tree."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "NODE_SIZE": {
            "description": [
                "The minimum number of data in a leaf."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ETA": {
            "description": [
                "Learning rate of each iteration.",
                "Range: (0, 1)"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "GAMMA": {
            "description": [
                "The minimum loss change value to make a split in tree growth (gamma in the equation)."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "MIN_CHILD_HESSIAN": {
            "description": [
                "The minimum summation of sample weights (hessian) in the leaf node."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "NODE_WEIGHT_CONSTRAINT": {
            "description": [
                "The maximum weight constraint assigned to each tree node. 0 for no constraint."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "ROW_SAMPLE_RATE": {
            "description": [
                "The sample rate of row (data points).",
                "Range: (0, 1)"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "COL_SAMPLE_RATE_BYSPLIT": {
            "description": [
                "The sample rate of feature set in each split.",
                "Range: (0, 1)"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "COL_SAMPLE_RATE_BYTREE": {
            "description": [
                "The sample rate of feature set in each tree growth.",
                "Range: (0, 1)"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "LAMBDA": {
            "description": [
                "L2 regularization.",
                "Range: [0, 1]"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "ALPHA": {
            "description": [
                "L1 regularization.",
                "Range: [0, 1]"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "BASE_SCORE": {
            "description": [
                "Initial prediction score of all instances.",
                "Global bias for sufficient number of iterations.",
                "Range: (0, 1), only for binary classification."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "START_FROM_AVERAGE": {
            "description": [
                "Indicates whether to adopt the prior distribution as the initial point. To be specific, use average value if it is a regression problem, and use frequencies of labels if it is a classification problem.",
                "0: no",
                "1: yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "OBJ_FUNC": {
            "description": [
                "For regression:",
                "0: Squared error",
                "1: Squared log error",
                "2: Pseudo Huber error",
                "3: Gamma",
                "4: Tweedie",
                "9: Huber error"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "TWEEDIE_POWER": {
            "description": [
                "For Tweedie object function; must in range [1.0, 2.0]"
            ],
            "type": [
                "Double"
            ]
        },
        "HUBER_SLOPE": {
            "description": [
                "For Huber/Pseudo Huber object function; must be greater than 0."
            ],
            "type": [
                "Double"
            ]
        },
        "REPLACE_MISSING": {
            "description": [
                "Replace missing value by another value in the feature. If it is a continuous feature, the value is the mean value; if it is a categorical feature, the value is the most frequent one.",
                "  0: no",
                "  1: yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "DEFAULT_MISSING_DIRECTION": {
            "description": [
                "Define the default direction where missing value will go to while splitting.",
                "0: left",
                "1: right"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "FEATURE_GROUPING": {
            "description": [
                "Apply feature grouping by grouping sparse features that only contains one significant value in each row.",
                "0: no",
                "1: yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "TOLERANT_RATE": {
            "description": [
                "While applying feature grouping, still merge features when there are rows containing more than one significant value. This parameter specifies the rate of such rows allowed."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "FG_MIN_ZERO_RATE": {
            "description": [
                "Specifies the minimum zero rate that is used to indicate sparse columns, only which may take part in feature grouping."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "CALCULATE_IMPORTANCE": {
            "description": [
                "Determines whether to calculate variable importance:",
                "0: no",
                "1: yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "CALCULATE_CONFUSION_MATRIX": {
            "description": [
                "Determines whether to calculate confusion matrix:",
                "0: no",
                "1: yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "COMPRESSION": {
            "description": [
                "Indicate whether result model should be compressed:",
                "0: no",
                "1: yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_BITS": {
            "description": [
                "Maximal number of bits to quantize continuous features. Equivalent to use 2<sup>MAX_BITS</sup> bins."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "VALIDATION_SET_RATE": {
            "description": [
                "Specifies the rate of validation set that be sampled from data set. If 0.0 is set, then no early stop will be applied."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "STRATIFIED_VALIDATION_SET": {
            "description": [
                "Indicates whether to apply stratified method while sampling validation set."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "TOLERANT_ITER_NUM": {
            "description": [
                "Indicates how many consecutive deteriorated iterations should be observed before applying early stop."
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "GLM_Regressor": {
        "SOLVER": {
            "description": [
                "Specifies the optimisation algorithm",
                "irls: iterative re-weighted least square",
                "nr: Newton-Raphson method",
                "cd: coordinate descent, solves ENET problem"
            ],
            "type": [
                "VARCHAR",
                "NVARCHAR"
            ]
        },
        "FAMILY": {
            "description": [
                "Specifies the distribution:",
                "gaussian, normal",
                "poisson",
                "binomial",
                "gamma",
                "inversegaussian",
                "negativebinomial",
                "ordinal"
            ],
            "type": [
                "VARCHAR",
                "NVARCHAR"
            ]
        },
        "LINK": {
            "description": [
                "Specifies the link function:",
                "identity",
                "log",
                "logit",
                "probit",
                "comploglog",
                "reciprocal, inverse",
                "inversesquare",
                "sqrt"
            ],
            "type": [
                "VARCHAR",
                "NVARCHAR"
            ]
        },
        "HANDLE_MISSING": {
            "description": [
                "Specify how to handle missing covariates",
                "0: throw error",
                "1: remove missing rows",
                "2: replace missing value with 0"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "QUASI": {
            "description": [
                "Indicates whether to use the quasi-likelihood to estimate over-dispersion.",
                "0: Does not consider over-dispersion",
                "1: Considers over-dispersion"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "GROUP_RESPONSE": {
            "description": [
                "Indicates whether the response consists of two columns.",
                "0: Consists of only one column (the 2nd column)",
                "1: Consists of two columns (the 2nd and 3rd columns)"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_ITERATION": {
            "description": [
                "The maximum number of iterations for numeric optimization."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "CONVERGENCE_CRITERION": {
            "description": [
                "The convergence criterion of coefficients for numeric optimization."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "SIGNIFICANCE_LEVEL": {
            "description": [
                "The significance level for the confidence interval of estimated coefficients."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "OUTPUT_FITTED": {
            "description": [
                "Indicates whether to output the fitted response.",
                "0: Does not output the fitted response",
                "1: Outputs the fitted response"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ENET_ALPHA": {
            "description": [
                "The elastic net mixing parameter. The value range is between 0 and 1 inclusively."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "ENET_LAMBDA": {
            "description": [
                "Coefficient for L1 and L2 regularization."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "ENET_NUM_LAMBDA": {
            "description": [
                "The number of lambda values."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "LAMBDA_MIN_RATIO": {
            "description": [
                "The smallest value of lambda, as a fraction of the maximum lambda, where λmax is the smallest value for which all coefficients are zero."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "ORDERING": {
            "description": [
                "Specifies the categories orders for ordinal regression."
            ],
            "type": [
                "VARCHAR",
                "NVARCHAR"
            ]
        }
    },
    "EXP_Regressor": {
        "ALG": {
            "description": [
                "Specifies decomposition method:",
                "0: LU Decomposition (Fast, but requires matrix invertible, can not ensure numerical stable)",
                "1: QR Decomposition (Fast, does not require matrix invertible, numerical stable)",
                "2: Singular Value Decomposition (Slow, but does not require matrix invertible, numerical stable)",
                "5: Cholesky Decomposition (Very Fast, but requires matrix invertible, numerical stable)"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ADJUSTED_R2": {
            "description": [
                "0: Does not output adjusted R square",
                "1: Outputs adjusted R square"
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "DT_Regressor": {
        "ALGORITHM": {
            "description": [
                "Specifies the algorithm used to grow a decision tree.",
                "1: C45",
                "2: CHAID",
                "3: CART"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "PERCENTAGE": {
            "description": [
                "Specifies the percentage of the input data that will be used to build the tree model.",
                "For example, if you set this parameter to 0.7, then 70% of the training data will be used to build the tree model and 30% will be used to prune the tree model."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "MIN_RECORDS_OF_PARENT": {
            "description": [
                "Specifies the stop condition: if the number of records in one node is less than the specified value, the algorithm stops splitting."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MIN_RECORDS_OF_LEAF": {
            "description": [
                "Promises the minimum number of records in a leaf."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_DEPTH": {
            "description": [
                "Specifies the stop condition: if the depth of the tree model is greater than the specified value, the algorithm stops splitting."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "SPLIT_THRESHOLD": {
            "description": [
                "Specifies the stop condition for a node:",
                "C45: The information gain ratio of the best split is less than this value.",
                "CHAID: The p-value of the best split is greater than or equal to this value.",
                "CART: The reduction of Gini index or relative MSE of the best split is less than this value.",
                "The smaller the SPLIT_THRESHOLD value is, the larger a C45 or CART tree grows. On the contrary, CHAID will grow a larger tree with larger SPLIT_THRESHOLD value."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "*<name of target value>*_PRIOR_": {
            "description": [
                "Specifies the priori probability of every class label."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "DISCRETIZATION_TYPE": {
            "description": [
                "Specifies the strategy for discretizing continuous attributes:",
                "0: MDLPC",
                "1: Equal Frequency"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "*<column name>*_BIN_": {
            "description": [
                "Specifies the number of bins for discretisation"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_BRANCH": {
            "description": [
                "Specifies the maximum number of branches."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MERGE_THRESHOLD": {
            "description": [
                "Specifies the merge condition for CHAID: if the metric value is greater than or equal to the specified value, the algorithm will merge two branches."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "USE_SURROGATE": {
            "description": [
                "Indicates whether to use surrogate split when NULL values are encountered.",
                "0: Does not use surrogate split.",
                "1: Uses surrogate split."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "IS_OUTPUT_RULES": {
            "description": [
                "Specifies whether to output decision rules or not.",
                "0: Does not output decision rules.",
                "1: Outputs decision rules."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "IS_OUTPUT_CONFUSION_MATRIX": {
            "description": [
                "Specifies whether to output confusion matrix or not.",
                "0: Does not output confusion matrix.",
                "1: Outputs confusion matrix."
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "RDT_Regressor": {
        "TREES_NUM": {
            "description": [
                "Specifies the number of trees to grow."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "TRY_NUM": {
            "description": [
                "Specifies the number of randomly selected variables for splitting."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ALLOW_MISSING_DEPENDENT": {
            "description": [
                "Specifies if missing target value is allowed.",
                "0: Not allowed. An error occurs if missing dependent presents.",
                "1: Allowed. The datum with missing dependent is removed."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "NODE_SIZE": {
            "description": [
                "Specifies the minimum number of records in a leaf."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "SPLIT_THRESHOLD": {
            "description": [
                "Specifies the stop condition: if the improvement value of the best split is less than this value, the tree stops growing."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "CALCULATE_OOB": {
            "description": [
                "Indicates whether to calculate out-of-bag error.",
                "0: No",
                "1: Yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_DEPTH": {
            "description": [
                "The maximum depth of a tree."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "SAMPLE_FRACTION": {
            "description": [
                "The fraction of data used for training. Assume there are `n` pieces of data, SAMPLE_FRACTION is `r`, then `n*r` data is selected for training."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "STRATA": {
            "description": [
                "Specifies the stratified sampling for classification. This is a pair parameter, which means a parameter name corresponds to two values. The first value is the class label (INTEGER or VARCHAR/NVARCHAR), and the second one is the proportion of this class occupies in the sampling data. The class with a proportion less than 0 or larger than 1 will be ignored. If some classes are absent, they share the remaining proportion equally. If the summation of strata is less than 1, the remaining is shared by all classes equally."
            ],
            "type": [
                "(INTEGER, DOUBLE)",
                "(VARCHAR/NVARCHAR, DOUBLE)"
            ]
        },
        "PRIORS": {
            "description": [
                "Specifies the prior probabilities for classification. This is also a pair parameter. The first value is the class label (INTEGER or VARCHAR/NVARCHAR), and the second one is the prior probability of this class. The class with a prior probability less than 0 or larger than 1 will be ignored. If some classes are absent, they share the remaining probability equally. If the summation of priors is less than 1, the remaining is shared by all classes equally."
            ],
            "type": [
                "(INTEGER, DOUBLE)",
                "(VARCHAR/NVARCHAR, DOUBLE)"
            ]
        },
        "COMPRESSION": {
            "description": [
                "Specifies if the model is stored in compressed format",
                "0: No",
                "1: Yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_BITS": {
            "description": [
                "The maximum number of bits to quantize continuous features. Equivalent to use 2<sup>MAX_BITS</sup> bins to quantize the values of these continuous features. Reduce the size of bins may affect the precision of split values and the accuracy in prediction. Must be less than 31."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "QUANTIZE_RATE": {
            "description": [
                "This value decides whether to do quantization for split values of a certain continuous feature. If the largest frequency of these continuous split values is less than QUANTIZE_RATE, quantization method will be used to quantize the split values of the continuous feature."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "FITTINGS_QUANTIZATION": {
            "description": [
                "This parameter indicates whether to quantize fitting values (the values of leaves) in regression problem.",
                "0: No",
                "1: Yes",
                "It is recommended to use this technique for large dataset in regression problem."
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "MLP_Regressor": {
        "ACTIVATION": {
            "description": [
                "Active function code for the hidden layer.",
                "TANH = 1",
                "LINEAR = 2",
                "SIGMOID_ASYMMETRIC = 3",
                "SIGMOID_SYMMETRIC = 4",
                "GAUSSIAN_ASYMMETRIC = 5",
                "GAUSSIAN_SYMMETRIC = 6",
                "ELLIOT_ASYMMETRIC = 7",
                "ELLIOT_SYMMETRIC = 8",
                "SIN_ASYMMETRIC = 9",
                "SIN_SYMMETRIC = 10",
                "COS_ASYMMETRIC = 11",
                "COS_SYMMETRIC = 12",
                "ReLU = 13"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "OUTPUT_ACTIVATION": {
            "description": [
                "Active function code for the output layer. The code is the same as ACTIVATION."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "HIDDEN_LAYER_SIZE": {
            "description": [
                "Specifies the size of each hidden layer in the format of '2, 3, 4'. The value 0 will be ignored, for example, '2, 0, 3' is equal to '2, 3'."
            ],
            "type": [
                "NVARCHAR"
            ]
        },
        "MAX_ITER": {
            "description": [
                "Maximum number of iterations."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "TRAINING_STYLE": {
            "description": [
                "Specifies the training style:",
                "0: Batch",
                "1:Stochastic"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "LEARNING_RATE": {
            "description": [
                "Specifies the learning rate."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "MOMENTUM": {
            "description": [
                "Specifies the momentum factor."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "BATCH_SIZE": {
            "description": [
                "Specifies the size of mini batch."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "NORMALIZATION": {
            "description": [
                "Specifies the normalization type:",
                "0: None",
                "1: Z-transform",
                "2: Scalar"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "WEIGHT_INIT": {
            "description": [
                "Specifies the weight initial value:",
                "0: All zeros",
                "1: Normal distribution",
                "2: Uniform distribution in range (0, 1)",
                "3: Variance scale normal",
                "4: Variance scale uniform"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ONEHOT_MIN_FREQUENCY": {
            "description": [
                "Specifies the minimum frequency below which a category is considered infrequent."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ONEHOT_MAX_CATEGORIES": {
            "description": [
                "Specifies an upper limit of the number of output features for each input feature. ONEHOT_MAX_CATEGORIES includes the feature that combines infrequent categories."
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "MLR_Regressor": {
        "ALG": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies algorithms for solving the least square problem:",
                "1: QR decomposition",
                "2: SVD (numerically stable and can handle rank deficiency but computationally",
                "4: Cyclical coordinate descent method to solve elastic net regularized multiple linear regression",
                "5: Cholesky decomposition (fast but numerically unstable)",
                "6: Alternating direction method of multipliers (ADMM) to solve elastic net regularized multiple linear regression. This method is faster than the cyclical coordinate descent method in many cases and is recommended."
            ]
        },
        "VARIABLE_SELECTION": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "0: All variables are included",
                "1: Forward selection",
                "2: Backward selection",
                "3: Stepwise selection"
            ]
        },
        "MANDATORY_FEATURE": {
            "type": [
                "VARCHAR"
            ],
            "description": [
                "Specifies the column name that needs to be included in the final training model when executing the variable selection. This parameter can be specified multiple times, each time with one column name as feature.",
                "### Note: This parameter is a hint. There are exceptional cases that the specified mandatory feature is excluded in the final model, for instance, some mandatory features can be represented as a linear combination of other features, among which some are also mandatory features."
            ]
        },
        "NO_INTERCEPT": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies whether the intercept term should be ignored in the model.",
                "0: Does not ignore the intercept term",
                "1: Ignores the intercept term"
            ]
        },
        "ALPHA_TO_ENTER": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "P-value for forward selection and stepwise selection."
            ]
        },
        "ALPHA_TO_REMOVE": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "P-value for backward selection and stepwise selection."
            ]
        },
        "ENET_LAMBDA": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Penalized weight. The value should be equal to or greater than 0."
            ]
        },
        "ENET_ALPHA": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The elastic net mixing parameter. The value range is between 0 and 1 inclusively.",
                "0: Ridge penalty",
                "1: LASSO penalty"
            ]
        },
        "HANDLE_MISSING": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Whether to handle missing value:",
                "0: No",
                "1: Yes"
            ]
        },
        "MAX_ITERATION": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Maximum number of passes over training data."
            ]
        },
        "THRESHOLD": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Convergence threshold for coordinate descent."
            ]
        },
        "PHO": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Step size for ADMM. Generally, the value should be greater than 1."
            ]
        },
        "STAT_INF": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies whether to output t-value and Pr(>|t|) of coefficients in the result table or not.",
                "0: No",
                "1: Yes"
            ]
        },
        "ADJUSTED_R2": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies whether to output adjusted R square or not.",
                "0: No",
                "1: Yes"
            ]
        },
        "DW_TEST": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies whether to do Durbin-Watson test under the null hypothesis that the errors do not follow a first order autoregressive process.",
                "0: No",
                "1: Yes"
            ]
        },
        "RESET_TEST": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies the order of Ramsey RESET test. Choosing 1 means this test will not be conducted. If you specify an INTEGER larger than 1, then the MLR function will run Ramsey RESET test with power of variables ranging from 2 to the value you specified."
            ]
        },
        "BP_TEST": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies whether or not to do Breusch-Pagan test under the null hypothesis that homoscedasticity is satisfied.",
                "0: No",
                "1: Yes"
            ]
        },
        "KS_TEST": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies whether or not to do Kolmogorov-Smirnov normality test under the null hypothesis that if errors of MLR follow a normal distribution.",
                "0: No",
                "1: Yes"
            ]
        },
        "ONEHOT_MIN_FREQUENCY": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies the minimum frequency below which a category is considered infrequent."
            ]
        },
        "ONEHOT_MAX_CATEGORIES": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies an upper limit of the number of output features for each input feature. ONEHOT_MAX_CATEGORIES includes the feature that combines infrequent categories."
            ]
        }
    },
    "GEO_Regressor": {
        "ALG": {
            "description": [
                "Specifies decomposition method:",
                "0: LU Decomposition (Fast, but requires matrix invertible, can not ensure numerical stable)",
                "1: QR Decomposition (Fast, does not require matrix invertible, numerical stable)",
                "2: Singular Value Decomposition (Slow, but does not require matrix invertible, numerical stable)",
                "5: Cholesky Decomposition (Very Fast, but requires matrix invertible, numerical stable)"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ADJUSTED_R2": {
            "description": [
                "0: Does not output adjusted R square",
                "1: Outputs adjusted R square"
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "LOG_Regressor": {
        "ALG": {
            "description": [
                "Specifies decomposition method:",
                "0: LU Decomposition (Fast, but requires matrix invertible, can not ensure numerical stable)",
                "1: QR Decomposition (Fast, does not require matrix invertible, numerical stable)",
                "2: Singular Value Decomposition (Slow, but does not require matrix invertible, numerical stable)",
                "5: Cholesky Decomposition (Very Fast, but requires matrix invertible, numerical stable)"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ADJUSTED_R2": {
            "description": [
                "0: Does not output adjusted R square",
                "1: Outputs adjusted R square"
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "POL_Regressor": {
        "POLYNOMIAL_NUM": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "This is a mandatory parameter to create a polynomial of degree POLYNOMIAL_NUM model.",
                "Note: POLYNOMIAL_NUM replaces VARIABLE_NUM."
            ]
        },
        "ALG": {
            "description": [
                "Specifies decomposition method:",
                "0: LU Decomposition (Fast, but require matrix invertible, can not ensure numerical stable)",
                "1: QR Decomposition (Fast, do not require matrix invertible, numerical stable)",
                "2: Singular Value Decomposition (Slow, but do not require matrix invertible, numerical stable)",
                "5: Cholesky Decomposition (Very Fast, but require matrix invertible, numerical stable)"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ADJUSTED_R2": {
            "description": [
                "0: Does not output adjusted R square",
                "1: Outputs adjusted R square"
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "SVM_Regressor": {
        "KERNEL_TYPE": {
            "description": [
                "Kernel type:",
                "0: LINEAR KERNEL",
                "1: POLY KERNEL",
                "2: RBF KERNEL",
                "3: SIGMOID KERNEL"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "POLY_DEGREE": {
            "description": [
                "Coefficient for the PLOY KERNEL type.",
                "Value range: ≥ 1"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "RBF_GAMMA": {
            "description": [
                "Coefficient for the RBF KERNEL type.",
                "Value range: > 0"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "COEF_LIN": {
            "description": [
                "Coefficient for the POLY/SIGMOID KERNEL type."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "COEF_CONST": {
            "description": [
                "Coefficient for the POLY/SIGMOID KERNEL type."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "SVM_C": {
            "description": [
                "Trade-off between training error and margin.",
                "Value range: > 0"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "SCALE_INFO": {
            "description": [
                "No scale",
                "1: Standardization",
                "The algorithm transforms the data to have zero mean and unit variance. The general formula is given as:",
                "where x is the origin feature vector, μ is the mean of that feature vector, and σ is its standard deviation.",
                "2: Rescaling",
                "The algorithm rescales the range of the features to scale the range in [-1,1]. The general formula is given as:",
                "where x is the origin feature vector."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "SCALE_LABEL": {
            "description": [
                "Controls whether to standardize the label for SVR.",
                "0: No",
                "1: Yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "SHRINK": {
            "description": [
                "Decides whether to use shrink strategy or not:",
                "0: Does not use shrink strategy",
                "1: Uses shrink strategy"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "HANDLE_MISSING": {
            "description": [
                "Whether to handle missing values:",
                "0: No",
                "1: Yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "CATEGORY_WEIGHT": {
            "description": [
                "Represents the weight of category attributes (γ). The value must be greater than 0."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "TOL": {
            "description": [
                "Specifies the error tolerance in the training process. The value must be greater than 0."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "EVALUATION_SEED": {
            "description": [
                "The random seed in parameter selection. The value must be greater than 0."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "COMPRESSION": {
            "description": [
                "Specifies if the model is stored in compressed format.",
                "0 : no",
                "1: yes"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_BITS": {
            "description": [
                "The maximum number of bits to quantize continuous features. Equivalent to use 2^MAX_BITS bins. Must be less than 31."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_QUANTIZATION_ITER": {
            "description": [
                "The maximum iteration steps for quantization."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ONEHOT_MIN_FREQUENCY": {
            "description": [
                "Specifies the minimum frequency below which a category is considered infrequent."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ONEHOT_MAX_CATEGORIES": {
            "description": [
                "Specifies an upper limit of the number of output features for each input feature. ONEHOT_MAX_CATEGORIES includes the feature that combines infrequent categories."
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "Outlier": {
        "WINDOW_SIZE": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Odd number, the window size for median filter, not less than 1. The value 1 means median filter is not applied."
            ]
        },
        "OUTLIER_METHOD": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "The method to calculate the outlier score from residual.",
                "0: Z1 score",
                "1: Z2 score",
                "2: IQR score",
                "3: MAD score",
                "4: Isolation Forest score",
                "5: DBSCAN"
            ]
        },
        "THRESHOLD": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The threshold for outlier score. If the absolute value of outlier score is beyond the threshold, PAL considers the corresponding data point as an outlier."
            ]
        },
        "THRESHOLD_ISOLATION": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The threshold for outlier score of isolation forest. If the absolute value of outlier score is beyond the threshold, PAL considers the corresponding data point as an outlier."
            ]
        },
        "DETECT_SEASONALITY": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "When calculating the residual,",
                "0: Does not consider the seasonal decomposition.",
                "1: Considers the seasonal decomposition."
            ]
        },
        "ALPHA": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The criterion for the autocorrelation coefficient. The value range is (0, 1). A larger value indicates stricter requirement for seasonality."
            ]
        },
        "EXTRAPOLATION": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies whether to extrapolate the endpoints.",
                "0: No",
                "1: Yes"
            ]
        },
        "SEED": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies the seed for random number generator.",
                "0: Uses the current time (in second) as seed.",
                "Others: Uses the specified value as seed."
            ]
        },
        "N_ESTIMATORS": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies the number of trees to grow."
            ]
        },
        "MAX_SAMPLES": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies the number of samples to draw from input to train each tree. If MAX_SAMPLES is larger than the number of samples provided, all samples will be used for all trees."
            ]
        },
        "BOOTSTRAP": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies sampling method.",
                "0: Sampling without replacement.",
                "1: Sampling with replacement."
            ]
        },
        "MINPTS": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies the minimum number of points required to form a cluster. The point itself is not included in MINPTS."
            ]
        },
        "RADIUS": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Specifies the scan radius (eps)."
            ]
        },
        "IMPUTE_FLAG": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies whether to impute after detecting outliers. Valid values are: 0, 1. The imputation method is to drop the residual of outlier points."
            ]
        }
    },
    "ImputeTS": {
        "IMPUTATION_TYPE": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Chooses the overall default imputation type for all columns:",
                "0: NONE",
                "1: MODE_ALLZERO",
                "2: MODE_MEAN",
                "3: MODE_MEDIAN",
                "4: MODE_SMA",
                "5: MODE_LMA",
                "6: MODE_EMA",
                "7: MODE_LINTERP",
                "8: MODE_SINTERP",
                "9: MODE_SEADEC",
                "10: MODE_LOCF",
                "11: MODE_NOCB"
            ]
        },
        "*<column name>*_IMPUTATION_TYPE": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Sets imputation type of a specified column. This value overwrites the default imputation type deduced by the IMPUTATION_TYPE parameter:",
                "0: NONE",
                "1: MODE",
                "2: ALLZERO",
                "3: MEAN",
                "4: MEDIAN",
                "5: SPECIFIED_CATEGORICAL_VALUE",
                "6: SPECIFIED_NUMERICAL_VALUE",
                "7: SIMPLE_MOVING_AVG",
                "8: LINEAR_MOVING_AVG",
                "9: EXP_MOVING_AVG",
                "10: LINEAR_INTERP",
                "11: SPLINE_INTERP",
                "12: SEADEC",
                "13: LOCF",
                "14: NOCB"
            ]
        },
        "BASE_ALGORITHM": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Chooses the base imputation method when imputation type is `SEADEC`:",
                "0: ALLZERO",
                "1: MEAN",
                "2: MEDIAN",
                "3: SIMPLE_MOVING_AVG",
                "4: LINEAR_MOVING_AVG",
                "5: EXP_MOVING_AVG",
                "6: LINEAR_INTERP",
                "7: SPLINE_INTERP",
                "8: LOCF",
                "9: NOCB"
            ]
        },
        "ALPHA": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The criterion for the autocorrelation coefficient. The value range is (0, 1). A larger value indicates stricter requirement for seasonality."
            ]
        },
        "EXTRAPOLATION": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies whether to extrapolate the endpoints.",
                "0: No",
                "1: Yes"
            ]
        },
        "SMOOTH_WIDTH": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies the width of the moving average applied to non-seasonal data. 0 indicates linear fitting to extract trends."
            ]
        },
        "AUXILIARY_NORMALITYTEST": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Specifies whether to use normality test to identify model types.",
                "0: No",
                "1: Yes"
            ]
        }
    },
    "BSTS": {
        "BURN_IN": {
            "description": [
                "Indicates the ratio of total MCMC draws that be ignored at the beginning. Ranging from 0 to 1."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "EXPECTED_MODEL_SIZE": {
            "description": [
                "Indicates the number of contemporaneous data that expected to be included in the model."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "SEASONAL_PERIOD": {
            "description": [
                "Indicates the Value of the seasonal period.",
                "- Negative value: Period determined automatically",
                "- 0 and 1: Target time series is assumed non-seasonal",
                "- 2 and larger: Period of target time series is set as the corresponding value"
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_ITER": {
            "description": [
                "Indicates the total number of MCMC draws."
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "ARIMA": {
        "SEASONAL_PERIOD": {
            "description": [
                "Value of the seasonal period. Negative: Automatically identify seasonality by means of auto-correlation scheme. 0 or 1: Non-seasonal. Others: Seasonal period."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "SEASONALITY_CRITERION": {
            "description": [
                "The criterion of the auto-correlation coefficient for accepting seasonality, in the range of (0, 1). The larger it is, the less probable a time series is regarded to be seasonal."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "D": {
            "description": [
                "Order of first-differencing. Others: Uses the specified value as the first-differencing order. Negative: Automatically identifies first-differencing order with KPSS test."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "KPSS_SIGNIFICANCE_LEVEL": {
            "description": [
                "The significance level for KPSS test. Supported values are 0.01, 0.025, 0.05, and 0.1. The smaller it is, the larger probable a time series is considered as first-stationary, that is, the less probable it needs first-differencing."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "MAX_D": {
            "description": [
                "The maximum value of D when KPSS test is applied."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "SEASONAL_D": {
            "description": [
                "Order of seasonal-differencing. Negative: Automatically identifies seasonal-differencing order Canova-Hansen test. Others: Uses the specified value as the seasonal-differencing order."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "CH_SIGNIFICANCE_LEVEL": {
            "description": [
                "The significance level for Canova-Hansen test. Supported values are 0.01, 0.025, 0.05, 0.1, and 0.2. The smaller it is, the larger probable a time series is considered seasonal-stationary, that is, the less probable it needs seasonal-differencing."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "MAX_SEASONAL_D": {
            "description": [
                "The maximum value of SEASONAL_D when Canova-Hansen test is applied."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_P": {
            "description": [
                "The maximum value of AR order p."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_Q": {
            "description": [
                "The maximum value of MA order q."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_SEASONAL_P": {
            "description": [
                "Negative: Automatically identifies first-differencing order. The maximum value of SAR order P."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_SEASONAL_Q": {
            "description": [
                "The maximum value of SMA order Q."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "INFORMATION_CRITERION": {
            "description": [
                "The information criterion for order selection. 0: AICC. 1: AIC. 2: BIC."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "SEARCH_STRATEGY": {
            "description": [
                "The search strategy for optimal ARMA model. 0: Exhaustive traverse. 1: Stepwise traverse."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_ORDER": {
            "description": [
                "The maximum value of (p + q + P + Q)."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "INITIAL_P": {
            "description": [
                "Order p of user-defined initial model."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "INITIAL_Q": {
            "description": [
                "Order q of user-defined initial model."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "INITIAL_SEASONAL_P": {
            "description": [
                "Order P of user-defined initial model."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "INITIAL_SEASONAL_Q": {
            "description": [
                "Order Q of user-defined initial model."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "GUESS_STATES": {
            "description": [
                "If employing ACF/PACF to guess initial ARMA models, besides user-defined model: 0: No guess. Besides user-defined model, uses states (2, 2) (1, 1)m, (1, 0) (1, 0)m, and (0, 1) (0, 1)m meanwhile as starting states. 1: Guesses starting states taking advantage of ACF/PACF."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_SEARCH_ITERATIONS": {
            "description": [
                "The maximum iterations for searching optimal ARMA states."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "METHOD": {
            "description": [
                "The object function for numeric optimization: 0: Uses the conditional sum of squares (CSS). 1: Uses the maximized likelihood estimation (MLE). 2: Firstly uses CSS to approximate starting values, and then uses MLE to fit (CSS-MLE)."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ALLOW_LINEAR": {
            "description": [
                "Controls whether to check linear model ARMA(0,0)(0,0)m. 0: No. 1: Yes."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "FORECAST_METHOD": {
            "description": [
                "Store information for the subsequent forecast method.",
                "0: Formula forecast.",
                "1: Innovations algorithm."
            ],
            "type": [
                "INTEGER"
            ]
        }
    },
    "AMTSA": {
        "GROWTH": {
            "type": [
                "VARCHAR, NVARCHAR"
            ],
            "description": [
                "Specify a trend:",
                "`linear`: linear trend",
                "`logistic`: logistic trend"
            ]
        },
        "CAP": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Specify the carrying capacity for logistic growth."
            ]
        },
        "SEASONALITY_MODE": {
            "type": [
                "VARCHAR, NVARCHAR"
            ],
            "description": [
                "Specify a seasonality mode: `additive`, `multiplicative`."
            ]
        },
        "NUM_CHANGEPOINTS": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Number of potential change points to include."
            ]
        },
        "CHANGEPOINT_RANGE": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Proportion of history in which trend change points are estimated."
            ]
        },
        "SEASONALITY": {
            "type": [
                "VARCHAR, NVARCHAR"
            ],
            "description": [
                "Add seasonality to model in a json format, including NAME, PERIOD, FOURIER_ORDER, PRIOR_SCALE, and MODE elements. For example:",
                "'{ \"NAME\": \"MONTHLY\", \"PERIOD\":30, \"FOURIER_ORDER\":5 }'"
            ]
        },
        "REGRESSOR": {
            "type": [
                "VARCHAR, NVARCHAR"
            ],
            "description": [
                "Specify the regressor in a json format, including PRIOR_SCALE, STANDARDIZE, and MODE elements. For example:",
                "'{ \"NAME\": \"X1\", \"PRIOR_SCALE\":4, \"MODE\": \"additive\" }'"
            ]
        },
        "CHANGE_POINT": {
            "type": [
                "VARCHAR, NVARCHAR"
            ],
            "description": [
                "Specify change point location in a timestamp format, such as '2019-01-01 00:00:00'."
            ]
        },
        "YEARLY_SEASONALITY": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Fit yearly seasonality:",
                "-1: auto",
                "0: false",
                "1: true"
            ]
        },
        "WEEKLY_SEASONALITY": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Fit weekly seasonality:",
                "-1: auto",
                "0: false",
                "1: true"
            ]
        },
        "DAILY_SEASONALITY": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Fit daily seasonality:",
                "-1: auto",
                "0: false",
                "1: true"
            ]
        },
        "SEASONALITY_PRIOR_SCALE": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Parameter modulating the strength of the seasonality model."
            ]
        },
        "HOLIDAYS_PRIOR_SCALE": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Parameter modulating the strength of the holiday components model."
            ]
        },
        "CHANGEPOINT_PRIOR_SCALE": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Parameter modulating the flexibility of the automatic change point selection."
            ]
        }
    },
    "MLR_TimeSeries": {
        "ALG": {
            "description": [
                "Specifies algorithms for solving the least square problem:",
                "1: QR decomposition",
                "2: SVD (numerically stable and can handle rank deficiency but computationally)",
                "4: Cyclical coordinate descent method to solve elastic net regularized multiple linear regression",
                "5: Cholesky decomposition (fast but numerically unstable)",
                "6: Alternating direction method of multipliers (ADMM) to solve elastic net regularized multiple linear regression. This method is faster than the cyclical coordinate descent method in many cases and is recommended."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MAX_ITERATION": {
            "description": [
                "Maximum number of passes over training data."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "THRESHOLD": {
            "description": [
                "Convergence threshold for coordinate descent."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "ENET_LAMBDA": {
            "description": [
                "Penalized weight. The value should be equal to or greater than 0."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "ENET_ALPHA": {
            "description": [
                "The elastic net mixing parameter. The value range is between 0 and 1 inclusively.",
                "0: Ridge penalty",
                "1: LASSO penalty"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "VAR_SELECT": {
            "description": [
                "0: All variables are included;",
                "1: Forward selection;",
                "2: Backward selection;",
                "3: Stepwise selection."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "MANDATORY_FEATURE": {
            "description": [
                "Specifies the column name that needs to be included in the final training model when executing the variable selection. This parameter can be specified multiple times, each time with one column name as feature."
            ],
            "type": [
                "VARCHAR"
            ]
        },
        "INTERCEPT": {
            "description": [
                "Specifies whether the intercept term should be ignored in the model.",
                "0: Do not ignore;",
                "1: Ignore."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ALPHA_TO_ENTER": {
            "description": [
                "P-value for forward selection and stepwise selection."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "ALPHA_TO_REMOVE": {
            "description": [
                "P-value for backward selection and stepwise selection."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "HANDLE_MISSING": {
            "description": [
                "Whether to handle missing value."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "PHO": {
            "description": [
                "Step size for ADMM. Generally, the value should be greater than 1."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "STAT_INF": {
            "description": [
                "Specifies whether to output t-value and Pr(>|t|) of coefficients in the result table or not."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ADJUSTED_R2": {
            "description": [
                "Specifies whether to output adjusted R square or not."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "DW_TEST": {
            "description": [
                "Specifies whether to do Durbin-Watson test under the null hypothesis that the errors do not follow a first order autoregressive process."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "RESET_TEST": {
            "description": [
                "Specifies the order of Ramsey RESET test. Choosing 1 means this test will not be conducted. If you specify an INTEGER larger than 1, then the MLR function will run Ramsey RESET test with power of variables ranging from 2 to the value you specified."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "BP_TEST": {
            "description": [
                "Specifies whether or not to do Breusch-Pagan test under the null hypothesis that homoscedasticity is satisfied."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "KS_TEST": {
            "description": [
                "Specifies whether or not to do Kolmogorov-Smirnov normality test under the null hypothesis that if errors of MLR follow a normal distribution."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ONEHOT_MIN_FREQUENCY": {
            "description": [
                "Specifies the minimum frequency below which a category is considered infrequent."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ONEHOT_MAX_CATEGORIES": {
            "description": [
                "Specifies an upper limit of the number of output features for each input feature. ONEHOT_MAX_CATEGORIES includes the feature that combines infrequent categories."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "LAG": {
            "description": [
                "The number of previous time stamps whose data is used for generating features in the current time stamp."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "LAG_FEATURES": {
            "description": [
                "The name of feature columns in time series data used for feature engineering using the LAG function in HGBT_TimeSeries or MLR_TimeSeries. The name of the target column should not be included."
            ],
            "type": [
                "VARCHAR",
                "NVARCHAR"
            ]
        }
    },
    "HGBT_TimeSeries": {
        "ITER_NUM": {
            "description": [
                "Total iteration number, which is equivalent to the number of trees in the final model."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "ETA": {
            "description": [
                "Learning rate of each iteration.",
                "Range: (0, 1)"
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "MIN_CHILD_HESSIAN": {
            "description": [
                "The minimum summation of sample weights (hessian) in the leaf node."
            ],
            "type": [
                "DOUBLE"
            ]
        },
        "SPLIT_METHOD": {
            "type": [
                "VARCHAR",
                "NVARCHAR"
            ],
            "description": [
                "The method to find split point for numeric feature.",
                "exact: the exact method, trying all possible points;",
                "sketch: the sketch method, accounting for the distribution of the sum of hessian;",
                "sampling: samples the split point randomly;",
                "histogram:builds histogram upon data and uses it as split point."
            ]
        },
        "SKETCH_EPS": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The epsilon of the sketch method. It indicates that the sum of hessian between two split points is not larger than this value. That is, the number of bins is approximately 1/eps."
            ]
        },
        "MAX_BIN_NUM": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "The maximum bin number for histogram method. Decreasing this number gains better performance in terms of running time at a cost of accuracy."
            ]
        },
        "N_ESTIMATORS": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Total iteration number, which is equivalent to the number of trees in the final model."
            ]
        },
        "MAX_DEPTH": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "The maximum depth of each tree."
            ]
        },
        "MIN_SAMPLES_LEAF": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "The minimum number of data in a leaf."
            ]
        },
        "LEARNING_RATE": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Learning rate of each iteration. Range: (0, 1)."
            ]
        },
        "SPLIT_THRESHOLD": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The minimum loss change value to make a split in tree growth (gamma in the equation)."
            ]
        },
        "MIN_SAMPLES_WEIGHT_LEAF": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The minimum summation of sample weights(hessian) in the leaf node."
            ]
        },
        "MAX_W_IN_SPLIT": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The maximum weight constraint assigned to each tree node. 0 for no constraint."
            ]
        },
        "SUBSAMPLE": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The sample rate of row (data points). Range: (0, 1)."
            ]
        },
        "COL_SUBSAMPLE_SPLIT": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The sample rate of feature set in each split. Range: (0, 1)."
            ]
        },
        "COL_SUBSAMPLE_TREE": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The sample rate of feature set in each tree growth. Range: (0, 1)."
            ]
        },
        "LAMB": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "L2 regularization. Range: [0, 1]."
            ]
        },
        "ALPHA": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "L1 regularization. Range: [0, 1]"
            ]
        },
        "BASE_SCORE": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Initial prediction score of all instances. Global bias for sufficient number of iterations."
            ]
        },
        "START_FROM_AVERAGE": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Indicates whether to adopt the prior distribution as the initial point. To be specific, use average value if it is a regression problem.",
                "0: no;",
                "1: yes."
            ]
        },
        "CALCULATE_IMPORTANCE": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Determines whether to calculate variable importance:",
                "0: no;",
                "1: yes."
            ]
        },
        "OBJ_FUNC": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "For regression:",
                "0: Squared error",
                "1: Squared log error",
                "2: Pseudo Huber error",
                "3: Gamma",
                "4: Tweedie"
            ]
        },
        "TWEEDIE_POWER": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "For Tweedie object function; must be in range [1.0, 2.0]."
            ]
        },
        "REPLACE_MISSING": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Replace missing value by another value in the feature.",
                "If it is a continuous feature, the value is the mean value;",
                "If it is a categorical feature, the value is the most frequent one.",
                "0: no",
                "1: yes"
            ]
        },
        "DEFAULT_MISSING_DIRECTION": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Define the default direction where missing value will go to while splitting.",
                "0: left",
                "1: right"
            ]
        },
        "LAG": {
            "description": [
                "The number of previous time stamps whose data is used for generating features in the current time stamp."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "LAG_FEATURES": {
            "description": [
                "The name of feature columns in time series data used for feature engineering using the LAG function in HGBT_TimeSeries or MLR_TimeSeries. The name of the target column should not be included."
            ],
            "type": [
                "VARCHAR",
                "NVARCHAR"
            ]
        }
    },
    "AutoExpSm": {
        "MODELSELECTION": {
            "description": [
                "When this is set to 1, the algorithm will select the best model among Single/Double/Triple/Damped Double/Damped Triple Exponential Smoothing models."
            ],
            "type": [
                "INTEGER"
            ]
        },
        "OPTIMIZER_TIME_BUDGET": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Time budget for Nelder-Mead optimization process. The time unit is second and the value should be larger than zero."
            ]
        },
        "MAX_ITERATION": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Maximum number of iterations for simulated annealing."
            ]
        },
        "OPTIMIZER_RANDOM_SEED": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Random seed for simulated annealing. The value should be larger than zero."
            ]
        },
        "ALPHA": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Weight for smoothing.",
                "Value range: 0 < α < 1"
            ]
        },
        "BETA": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Weight for the trend component.",
                "Value range: 0 Value range: 0 ≤ β < 1",
                "**Note**: If it is not set, the optimized value will be computed automatically."
            ]
        },
        "GAMMA": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Weight for the seasonal component.",
                "Value range: 0 < γ < 1"
            ]
        },
        "PHI": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Value of the damped smoothing constant Φ (0 < Φ < 1)."
            ]
        },
        "FORECAST_NUM": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Number of values to be forecast."
            ]
        },
        "CYCLE": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Length of a cycle (L > 1). For example, the cycle of quarterly data is 4, and the cycle of monthly data is 12."
            ]
        },
        "SEASONAL": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "0: Multiplicative triple exponential smoothing",
                "1: Additive triple exponential smoothing"
            ]
        },
        "INITIAL_METHOD": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Initialization method for the trend and seasonal components. Refer to *Triple Exponential Smoothing* for detailed information on initialization method."
            ]
        },
        "TRAINING_RATIO": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The ratio of training data to the whole time series.",
                "Assuming the size of time series is N, and the training ratio is r, the first N*r time series is used to train, whereas only the latter N*(1-r) one is used to test."
            ]
        },
        "DAMPED": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "For DESM:",
                "0: Uses the Holt's linear method.",
                "1: Uses the additive damped trend Holt's linear method.",
                "For TESM:",
                "0: Uses the Holt Winter method.",
                "1: Uses the additive damped seasonal Holt Winter method."
            ]
        },
        "ACCURACY_MEASURE": {
            "type": [
                "VARCHAR"
            ],
            "description": [
                "The criterion used for the optimization. Available values are MSE and MAPE."
            ]
        },
        "SEASONALITY_CRITERION": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The criterion of the auto-correlation coefficient for accepting seasonality, in the range of (0, 1). The larger it is, the less probable a time series is regarded to be seasonal."
            ]
        },
        "TREND_TEST_METHOD": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "The method to test trend:",
                "1: MK test",
                "2: Difference-sign test"
            ]
        },
        "TREND_TEST_ALPHA": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Tolerance probability for trend test. The value range is (0, 0.5)."
            ]
        },
        "ALPHA_MIN": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Sets the minimum value of ALPHA."
            ]
        },
        "BETA_MIN": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Sets the minimum value of BETA."
            ]
        },
        "GAMMA_MIN": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Sets the minimum value of GAMMA."
            ]
        },
        "PHI_MIN": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Sets the minimum value of PHI."
            ]
        },
        "ALPHA_MAX": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Sets the maximum value of ALPHA."
            ]
        },
        "BETA_MAX": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Sets the maximum value of BETA."
            ]
        },
        "GAMMA_MAX": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Sets the maximum value of GAMMA."
            ]
        },
        "PHI_MAX": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Sets the maximum value of PHI."
            ]
        },
        "PREDICTION_CONFIDENCE_1": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Prediction confidence for interval 1"
            ]
        },
        "PREDICTION_CONFIDENCE_2": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Prediction confidence for interval 2"
            ]
        },
        "LEVEL_START": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The initial value for level component `S`. If this value is not provided, it will be calculated in the way as described in Triple Exponential Smoothing.",
                "**Note**: LEVEL_START cannot be zero. If it is set to zero, 0.0000000001 will be used instead.",
                "Refer to *Triple Exponential Smoothing* for detailed information"
            ]
        },
        "TREND_START": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The initial value for trend component `B`. If this value is not provided, it will be calculated in the way as described in Triple Exponential Smoothing.",
                "Refer to *Triple Exponential Smoothing* for detailed information"
            ]
        },
        "SEASON_START": {
            "type": [
                "INTEGER",
                "DOUBLE"
            ],
            "description": [
                "A list of initial values for seasonal component `C`. If this parameter is not used, the initial values of `C` will be calculated in the way as described in Triple Exponential Smoothing.",
                "Two values must be provided for each cycle:",
                "- Cycle ID: An INTEGER which represents which cycle the initial value is used for.",
                "- Initial value: A DOUBLE precision number which represents the initial value for the corresponding cycle.",
                "For example: To give the initial value 0.5 to the 3rd cycle, insert tuple ('SEASON_START', 3, 0.5, NULL) into the parameter table.",
                "**Note**: The initial values of all cycles must be provided if this parameter is used.",
                "Refer to *Triple Exponential Smoothing* for detailed information"
            ]
        }
    },
    "BrownExpSm": {
        "ALPHA": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "The smoothing constant alpha for brown exponential smoothing or the initialization value for adaptive brown exponential smoothing (0 < α < 1)."
            ]
        },
        "DELTA": {
            "type": [
                "DOUBLE"
            ],
            "description": [
                "Value of weighted for At and Mt."
            ]
        },
        "FORECAST_NUM": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "Number of values to be forecast."
            ]
        },
        "ADAPTIVE_METHOD": {
            "type": [
                "INTEGER"
            ],
            "description": [
                "0: Brown exponential smoothing",
                "1: Adaptive brown exponential smoothing"
            ]
        }
    },
    "CBEncoder": {}
}


OPERATOR_METADATA = {
    "Classifier": {
        "NB_Classifier": {
            "description": [
                "Naive Bayes"
            ],
            "type": ["Classification"]
        },
        "M_LOGR_Classifier": {
            "description": [
                "Multi-Class Logistic Regression"
            ],
            "type": ["Classification"]
        },
        "SVM_Classifier": {
            "description": [
                "Support Vector Machine"
            ],
            "type": ["Classification"]
        },
        "RDT_Classifier": {
            "description": [
                "Random Decision Trees"
            ],
            "type": ["Classification"]
        },
        "DT_Classifier": {
            "description": [
                "Decision Trees"
            ],
            "type": ["Classification"]
        },
        "HGBT_Classifier": {
            "description": [
                "Hybrid Gradient Boosting Tree"
            ],
            "type": ["Classification"]
        },
        "MLP_Classifier": {
            "description": [
                "Multilayer Perceptron"
            ],
            "type": ["Classification"]
        }
    },
    "Regressor": {
        "EXP_Regressor": {
            "description": [
                "Exponential Regression"
            ],
            "type": ["Regression"]
        },
        "GEO_Regressor": {
            "description": [
                "Bi-Variate Geometric Regression"
            ],
            "type": ["Regression"]
        },
        "GLM_Regressor": {
            "description": [
                "Generalized Linear Models"
            ],
            "type": ["Regression"]
        },
        "HGBT_Regressor": {
            "description": [
                "Hybrid Gradient Boosting Tree Regressor"
            ],
            "type": ["Regression"]
        },
        "LOG_Regressor": {
            "description": [
                "Bi-Variate Natural Logarithmic Regression"
            ],
            "type": ["Regression"]
        },
        "MLP_Regressor": {
            "description": [
                "Multilayer Perceptron Regressor"
            ],
            "type": ["Regression"]
        },
        "MLR_Regressor": {
            "description": [
                "Multiple Linear Regression"
            ],
            "type": ["Regression"]
        },
        "POL_Regressor": {
            "description": [
                "Polynomial Regression"
            ],
            "type": ["Regression"]
        },
        "RDT_Regressor": {
            "description": [
                "Random Decision Trees"
            ],
            "type": ["Regression"]
        },
        "DT_Regressor": {
            "description": [
                "Decision Trees"
            ],
            "type": ["Regression"]
        },
        "SVM_Regressor": {
            "description": [
                "Support Vector Machine for Regression"
            ],
            "type": ["Regression"]
        }
    },
    "Timeseries": {
        "SingleExpSm": {
            "description": [
                "Single Exponential Smoothing"
            ],
            "type": ["Timeseries"]
        },
        "DoubleExpSm": {
            "description": [
                "Double Exponential Smoothing"
            ],
            "type": ["Timeseries"]
        },
        "TripleExpSm": {
            "description": [
                "Triple Exponential Smoothing"
            ],
            "type": ["Timeseries"]
        },
        "BrownExpSm": {
            "description": [
                "Brown Exponential Smoothing"
            ],
            "type": ["Timeseries"]
        },
        "AutoExpSm": {
            "description": [
                "Auto Exponential Smoothing"
            ],
            "type": ["Timeseries"]
        },
        "BSTS": {
            "description": [
                "Bayesian Structural Time Series"
            ],
            "type": ["Timeseries"]
        },
        "ARIMA": {
            "description": [
                "Auto Regressive Integrated Moving Average"
            ],
            "type": ["Timeseries"]
        },
        "AMTSA": {
            "description": [
                "Additive Model Time Series Analysis"
            ],
            "type": ["Timeseries"]
        },
        "HGBT_TimeSeries": {
            "description": [
                "Hybrid Gradient Boosting Tree (regressor adapted for time series)"
            ],
            "type": ["Timeseries"]
        },
        "MLR_TimeSeries": {
            "description": [
                "Multiple Linear Regression (adapted for time series)"
            ],
            "type": ["Timeseries"]
        }
    },
    "Transformer": {
        "OneHotEncoder": {
            "description": [
                "Encodes categorical features with value between 1 and n_classes of each feature"
            ],
            "type": ["Classification", "Regression"]
        },
        "LabelEncoder": {
            "description": [
                "Encodes categorical features using a one-hot schema"
            ],
            "type": ["Classification", "Regression"]
        },
        "FS_unsupervised": {
            "description": [
                "Feature Selection for Unsupervised Learning"
            ],
            "type": ["Classification", "Regression"]
        },
        "FS_supervised": {
            "description": [
                "Feature Selection for Supervised Learning"
            ],
            "type": ["Classification", "Regression"]
        },
        "SCALE": {
            "description": [
                "Scaler"
            ],
            "type": ["Classification", "Regression"]
        },
        "CATPCA": {
            "description": [
                "Categorical Principal Component Analysis"
            ],
            "type": ["Classification", "Regression"]
        },
        "PolynomialFeatures": {
            "description": [
                "Generate polynomial features from existing features"
            ],
            "type": ["Classification", "Regression", "Timeseries"]
        },
        "CBEncoder": {
            "description": [
                "Encodes categorical features using Bayesian encoding"
            ],
            "type": ["Timeseries"]
        },
        "ImputeTS": {
            "description": [
                "Time Series Missing Value Handling"
            ],
            "type": ["Timeseries"]
        },
        "Imputer": {
            "description": [
                "Missing Value Handling"
            ],
            "type": ["Classification", "Regression"]
        }
    },
    "Resampler": {
        "SAMPLING": {
            "description": [
                "Sampling"
            ],
            "type": ["Classification", "Regression"]
        },
        "SMOTE": {
            "description": [
                "Synthetic minority over-sampling technique"
            ],
            "type": ["Classification", "Regression"]
        },
        "SMOTETomek": {
            "description": [
                "Over-sampling using SMOTE and cleaning using Tomek links"
            ],
            "type": ["Classification", "Regression"]
        },
        "TomekLinks": {
            "description": [
                "Tomek's Links"
            ],
            "type": ["Classification", "Regression"]
        },
        "Outlier": {
            "description": [
                "Outlier Detection"
            ],
            "type": ["Timeseries"]
        }
    },
    "Unknown": {}
}


def get_default_config_dict(identifier):
    file_name = 'config_dict_{}_default.json'.format(identifier)
    file_path = os.path.join(os.path.dirname(__file__), "..", "algorithms", "pal", "templates", file_name)
    with open(file_path) as json_file:
        return json.load(json_file)


CONFIG_TEMPLATE_DICT = {
    'classification': get_default_config_dict(identifier='classification'),
    'regression': get_default_config_dict(identifier='regression'),
    'timeseries': get_default_config_dict(identifier='timeseries')
}


CONFIG_TEMPLATE_DICT_STR = json.dumps({
    "Classification": CONFIG_TEMPLATE_DICT['classification'],
    "Regression": CONFIG_TEMPLATE_DICT['regression'],
    "Time Series": CONFIG_TEMPLATE_DICT['timeseries']
})


def get_temp_comm_file_path(iframe_id):
    return EmbeddedUI.get_resource_temp_file_path(iframe_id + '_config.json')


def save_code_by_python(iframe_id, config_str):
    EmbeddedUI.generate_file(get_temp_comm_file_path(iframe_id), config_str)


def convert_to_correct_type(config_dict):
    # "x" -> x | "x.0" -> x.0
    for operator in list(config_dict.keys()):
        for parameter in list(config_dict[operator].keys()):
            is_range_format = False
            values = config_dict[operator][parameter]
            if type(values) != list:
                is_range_format = True
                values = config_dict[operator][parameter]['range']

            new_values = []
            for v in values:
                if type(v) == str:
                    try:
                        v = int(v)
                    except:
                        try:
                            v = float(v)
                        except:
                            pass
                new_values.append(v)

            if is_range_format:
                config_dict[operator][parameter]['range'] = new_values
            else:
                config_dict[operator][parameter] = new_values


def get_correct_config_str(config_dict):
    if config_dict is None:
        config_dict = CONFIG_TEMPLATE_DICT['classification']
    else:
        if isinstance(config_dict, str) and config_dict in CONFIG_TEMPLATE_DICT:
            config_dict = CONFIG_TEMPLATE_DICT[config_dict]
        elif type(config_dict) == dict:
            pass
        else:
            raise TypeError("The value of parameter config_dict must be dict type or str type {}!".format(str(list(CONFIG_TEMPLATE_DICT))))
    input_config_str = json.dumps(config_dict)
    return input_config_str


def get_html_str(iframe_id, input_config_str, comm_server_url=''):
    html_str = EmbeddedUI.get_resource_template('automl_config.html').render(frameId=iframe_id,
                                                                             operator_metadata=OPERATOR_METADATA,
                                                                             parameter_metadata=PARAMETER_METADATA,
                                                                             input_config_str=input_config_str,
                                                                             config_template_dict_str=CONFIG_TEMPLATE_DICT_STR,
                                                                             comm_server=comm_server_url)
    return html_str


class AutoMLConfig(object):
    """
    Generates the AutoML Config UI.

    .. image:: image/automl_config_ui.png

    Parameters
    ----------
    config_dict : dict | str, optional
        Manually set the custom config_dict.

        If this parameter is not specified, the classification config template is used.

        Defaults to 'classification'.
    iframe_height : int, optional
        IFrame height.

        Defaults to 500.
    """
    def __init__(self, config_dict: Union[str, dict] = 'classification', iframe_height: int = 500) -> None:
        self.input_config_str = get_correct_config_str(config_dict)
        self.runtime_platform = EmbeddedUI.get_runtime_platform()[1]
        self.iframe_id = EmbeddedUI.get_uuid()
        self.temp_comm_file_path = get_temp_comm_file_path(self.iframe_id)

        if self.runtime_platform == 'databricks':
            from hana_ml.visualizers.server import CommServerManager
            comm_server_manager = CommServerManager()
            comm_server_manager.start()
            comm_server_url = comm_server_manager.get_comm_server_url()

            self.html_str = get_html_str(self.iframe_id, self.input_config_str, quote(comm_server_url, safe=':/?=&'))
            EmbeddedUI.generate_file(EmbeddedUI.get_resource_temp_file_path("{}.html".format(self.iframe_id)), self.html_str)

            print('Page URL: {}/page?id={}&type=AutoMLConfigUI'.format(comm_server_url, self.iframe_id))
            print("If you want to call method 'get_config_dict' successfully at any time, please first visit the page link above and do not close it.")
        else:
            self.html_str = get_html_str(self.iframe_id, self.input_config_str)
            EmbeddedUI.render_html_str(EmbeddedUI.get_iframe_str(self.html_str, self.iframe_id, iframe_height))
            time.sleep(2)

            if self.runtime_platform in ['vscode', 'bas']:
                print("\nIn order to call method 'get_config_dict' successfully on the VSCode or BAS platform, you must first import the VSCode extension package manually.")
                print('VSCode extension package path: \n{}'.format(EmbeddedUI.get_resource_temp_file_path('hanamlapi-monitor-1.2.0.vsix')))

    def get_config_dict(self):
        """
        Get the latest config dict from UI.

        Without Visual Studio Code Extension (VSIX), calling the get_config_dict method on VSCode or BAS platforms will fail.

        Follow the image below to install hanamlapi-monitor-1.2.0.vsix file on VSCode or BAS.

        - .. image:: image/import_vscode_extension_1.png

        - .. image:: image/import_vscode_extension_2.png

        - .. image:: image/import_vscode_extension_3.png

        - .. image:: image/import_vscode_extension_4.png
        """
        code = None

        if self.runtime_platform == 'databricks':
            # 1. write cmd to local file
            EmbeddedUI.generate_file(self.temp_comm_file_path, "get_config_dict")

            # 2. wait for response of UI
            time.sleep(10)

            # 3. get code from temp file
            retry_count = 1
            while retry_count <= 3:
                temp_comm_file_str = EmbeddedUI.get_file_str(self.temp_comm_file_path)
                if temp_comm_file_str == "get_config_dict":
                    retry_count = retry_count + 1
                    time.sleep(5)
                else:
                    code = json.loads(temp_comm_file_str)
                    break
        else:
            # 1. send cmd to ui
            if self.runtime_platform in ['jupyter', 'vscode', 'bas']:
                js_str = "for (let i = 0; i < window.length; i++) {const targetWindow = window[i];if(targetWindow['frameId']){if(targetWindow['frameId'] === '" + self.iframe_id + "'){" + "targetWindow['save_code_by_ui']();" + "}}}"
                EmbeddedUI.execute_js_str("{};".format(js_str))
                time.sleep(1)

            # 2. send cmd to vscode extension
            if self.runtime_platform in ['vscode', 'bas']:
                print('AutoML Config Temp Flag: {}: {}'.format(EmbeddedUI.get_resource_temp_dir_path() + os.sep, self.iframe_id))
                time.sleep(1)

            # 3. get code from temp file
            if self.runtime_platform in ['jupyter', 'vscode', 'bas']:
                retry_count = 1
                while retry_count <= 3:
                    temp_comm_file_str = EmbeddedUI.get_file_str(self.temp_comm_file_path)
                    if temp_comm_file_str:
                        code = json.loads(temp_comm_file_str)
                        break
                    else:
                        retry_count = retry_count + 1
                        time.sleep(5)

        # handle code or print warnings
        if code:
            del code['frameId']
            convert_to_correct_type(code)
        else:
            print("Calling method 'get_config_dict' failed!")
            print('Please try again or click the button on the UI to get the latest config code!')

        return code

    def generate_html(self, file_name):
        """
        Generate an HTML file.

        Parameters
        ----------
        file_name : str
            HTML file name.
        """
        html_str = self.html_str
        if self.runtime_platform == 'databricks':
            html_str = get_html_str(self.iframe_id, self.input_config_str)
        EmbeddedUI.generate_file('{}_automl_config.html'.format(file_name), html_str)
