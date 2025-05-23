import polars as pl
from typing import List, Dict, Optional

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report, brier_score_loss
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.model_selection import train_test_split, GridSearchCV

from imblearn.pipeline import Pipeline as ImbPipeline
from imblearn.over_sampling import SMOTE, ADASYN, RandomOverSampler


def create_model_pipeline(
    on_base_lf: pl.LazyFrame,
    responses: List[str],
    cat_predictors_drop: List[str] = [],
    cat_predictors_mode: List[str] = [],
    num_predictors_drop: List[str] = [],
    num_predictors_median: List[str] = [],
    model_type: str = "LogisticRegression",
    oversampling_method: str = "SMOTE",
    param_grid: Optional[Dict] = None,
    scoring: Dict = {'brier_score': 'neg_brier_score'},
    refit: str = "brier_score",
    cv: int = 5,
    test_size: float = 0.30,
    random_state: int = 123,
    verbose: bool = True):
    """
    Create and train a machine learning pipeline with preprocessing,
    oversampling, and model selection.
    
    Args:
        on_base_lf: Polars LazyFrame with the data
        responses: List of response variable column names
        cat_predictors_drop: Categorical predictors with drop imputation
        cat_predictors_mode: Categorical predictors with mode imputation  
        num_predictors_drop: Numerical predictors with drop imputation
        num_predictors_median: Numerical predictors with median imputation
        model_type: Type of model to use
        oversampling_method: Method for handling class imbalance
        param_grid: Parameters for GridSearchCV
        scoring: Scoring metric for model selection
        refit: Scoring metric when refitting on the full dataset
        cv: Cross-validation folds
        test_size: Proportion of data for testing
        random_state: Random seed for reproducibility
        verbose: Whether to print progress information
    
    Returns:
        dict: Contains trained pipeline, test data, predictions, and performance metrics
    """
    # ==== Preprocessing Pipeline ====
    # Column specific preprocessing steps
    numeric_median_pipeline = Pipeline([
        ("imputer", SimpleImputer(strategy="median")), 
        ("scaler", StandardScaler())
    ])
    
    numeric_drop_pipeline = Pipeline([
        ("scaler", StandardScaler())
    ])
    
    categorical_drop_pipeline = Pipeline([
        ("encoder", OneHotEncoder(handle_unknown="ignore"))
    ])
    
    categorical_mode_pipeline = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("encoder", OneHotEncoder(handle_unknown="ignore"))
    ])
    
    # Combine column specific preprocessing steps into a preprocessing pipeline
    transformers = []
    if num_predictors_drop:
        transformers.append(("num_drop", numeric_drop_pipeline, num_predictors_drop))
    if num_predictors_median:
        transformers.append(("num_median", numeric_median_pipeline, num_predictors_median))
    if cat_predictors_drop:
        transformers.append(("cat_drop", categorical_drop_pipeline, cat_predictors_drop))
    if cat_predictors_mode:
        transformers.append(("cat_mode", categorical_mode_pipeline, cat_predictors_mode))
    
    preprocessor = ColumnTransformer(transformers=transformers)
    
    # Over sampling selection
    if oversampling_method == "SMOTE":
        oversampler = SMOTE(random_state=random_state)
    elif oversampling_method == "ADASYN":
        oversampler = ADASYN(random_state=random_state)
    elif oversampling_method == "RandomOverSampler":
        oversampler = RandomOverSampler(random_state=random_state)
    else:
        raise ValueError(f"Unknown oversampling method: {oversampling_method}")
    
    # Model selection
    if model_type == "LogisticRegression":
        base_model = LogisticRegression(random_state=random_state)
    elif model_type == "RandomForestClassifier":
        base_model = RandomForestClassifier(random_state=random_state)
    elif model_type == "GradientBoostingClassifier":
        base_model = GradientBoostingClassifier(random_state=random_state)
    elif model_type == "KNeighborsClassifier":
        base_model = KNeighborsClassifier()
    elif model_type == "MLPClassifier":
        base_model = MLPClassifier(random_state=random_state)
    else:
        raise ValueError(f"Unknown model type: {model_type}")
    
    # Combine preprocessor, over sampler, and model into one pipeline
    pipeline = ImbPipeline([
        ("preprocessor", preprocessor),
        ("oversampler", oversampler),
        ("classifier", base_model)
    ])

    # For param_grid, start with classifier__{parameter} as the name.
    step = "classifier"
    param_grid = {f"{step}__{key}":val for key, val in param_grid.items()}
    
    # Wrap pipeline in a Gridsearch. Each CV set will have its own pipeline.
    grid_search = GridSearchCV(
        estimator=pipeline,
        param_grid=param_grid,
        cv=cv,
        scoring=scoring,
        verbose=1 if verbose else 0,
        n_jobs=-1,
        refit=refit
    )

    # ==== Data Preparation ====
    all_features = (responses + cat_predictors_drop + cat_predictors_mode + 
                    num_predictors_drop + num_predictors_median)
    all_predictors = (cat_predictors_drop + cat_predictors_mode + 
                      num_predictors_drop + num_predictors_median)
    drop_null_features = cat_predictors_drop + num_predictors_drop + responses
    
    if verbose:
        print(f"Total features: {len(all_features)}")
        print(f"Total Predictors: {len(all_predictors)}")
        print(f"Total Responses: {len(responses)}")
    
    # Select all features and perform drop imputation on specific columns
    on_base_pl = (on_base_lf
        .select(all_features)
        .drop_nulls(drop_null_features)
        .collect()
    )
    
    # Create predictor and response sets
    X = on_base_pl.select(all_predictors).to_pandas()
    y = on_base_pl.select(responses).to_pandas().squeeze()
    
    if verbose:
        print(f"\nDataset shape after drop imputation: {X.shape}")
    
    # Split the predictor and response sets into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, 
        test_size=test_size, 
        shuffle=True,
        stratify=y,
        random_state=random_state
    )
    
    # Train the model
    grid_search.fit(X_train, y_train)
    
    if verbose:
        best_param = [f"{param} = {value}" for param, value in grid_search.best_params_.items()]
        print(f"\nBest parameters: \n{"\n".join(best_param)}")
        print(f"\nBest cross-validation score: {grid_search.best_score_:.4f}\n")
    
    # ==== Model Evaluation ====
    best_pipeline = grid_search.best_estimator_
    y_pred = best_pipeline.predict(X_test)
    y_pred_proba = best_pipeline.predict_proba(X_test)[:, 1]
    
    # Calculate metrics
    brier_score = brier_score_loss(y_test, y_pred_proba)
    
    if verbose:
        print(classification_report(y_test, y_pred))
        print(f"Brier Score: {brier_score:.4f}")
        print(f"\nPredictors:\n{"\n".join(all_predictors)}")
        print(f"\nResponse:\n{"\n".join(responses)}")
    
    # Return important objects
    results = {
        'pipeline': best_pipeline,
        'grid_search': grid_search,
        'X_train': X_train,
        'X_test': X_test,
        'y_train': y_train,
        'y_test': y_test,
        'y_pred': y_pred,
        'y_pred_proba': y_pred_proba,
        'brier_score': brier_score,
        'best_params': grid_search.best_params_,
        'best_score': grid_search.best_score_,
        'feature_names': all_predictors,
        'response_names': responses
    }
    
    return results