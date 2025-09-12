"""
Model cards creation for PAL algorithms.
"""
#pylint: disable=protected-access, redefined-builtin
import logging
import os
from hana_ml.dataframe import DataFrame
try:
    from huggingface_hub import ModelCard, ModelCardData, RepoCard
except ImportError as error:
    logging.getLogger(__name__).error("%s: %s", error.__class__.__name__, str(error))
    pass

def _stats_to_json(pandas_df, massive):
    result = {}
    if massive:
        if len(pandas_df.columns) >= 4:
            for _, row in pandas_df.iterrows():
                result[row[pandas_df.columns[0]] + '_' + row[pandas_df.columns[1]] + '_' + row[pandas_df.columns[3]]] = row[pandas_df.columns[2]]
        else:
            for _, row in pandas_df.iterrows():
                result[row[pandas_df.columns[0]] + '_' + row[pandas_df.columns[1]]] = row[pandas_df.columns[2]]
    else:
        if len(pandas_df.columns) >= 3:
            for _, row in pandas_df.iterrows():
                result[row[pandas_df.columns[0]] + '_' + row[pandas_df.columns[2]]] = row[pandas_df.columns[1]]
        else:
            for _, row in pandas_df.iterrows():
                result[row[pandas_df.columns[0]]] = row[pandas_df.columns[1]]
    return result

def _get_model_stats(obj):
    model_info = {}
    if hasattr(obj, "training_data"):
        model_info["training_data"] = obj.training_data.select_statement
        model_info["training_data_signature"] = obj.training_data.get_table_structure()
    if hasattr(obj, "testing_data"):
        model_info["testing_data"] = obj.testing_data.select_statement
        model_info["testing_data_signature"] = obj.testing_data.get_table_structure()
    if hasattr(obj, "statistics_"):
        if obj.statistics_:
            massive = False
            if hasattr(obj, "massive"):
                massive = obj.massive
            model_info["model_metrics"] = _stats_to_json(obj.statistics_.collect(), massive)
    if hasattr(obj, "best_pipeline_"):
        if obj.best_pipeline_:
            model_info["model_metrics"] = obj.best_pipeline_.collect().iat[0, 2]
    if hasattr(obj, "score_metrics_"):
        if isinstance(obj.score_metrics_, DataFrame):
            massive = False
            if hasattr(obj, "massive"):
                massive = obj.massive
            score_metrics = obj.score_metrics_.collect()
            model_info["testing_metrics"] = _stats_to_json(score_metrics, massive)
        if isinstance(obj.score_metrics_, dict):
            model_info["testing_metrics"] = obj.score_metrics_
    if hasattr(obj, "scoring_list_"):
        if isinstance(obj.scoring_list_, (tuple, list)):
            massive = False
            if hasattr(obj, "massive"):
                massive = obj.massive
            score_metrics = obj.scoring_list_[1].collect()
            model_info["testing_metrics"] = _stats_to_json(score_metrics, massive)
    return model_info

def parse_model_card(model_card):
    """
    Parse a model card.

    Parameters
    ----------
    model_card : str
        The model card markdown.
    """
    return RepoCard(model_card)

def create_model_card(model,
                      language=None,
                      license=None,
                      library_name=None,
                      tags=None,
                      base_model=None,
                      datasets=None,
                      metrics=None,
                      eval_results=None,
                      model_name=None,
                      model_version=None,
                      card_data=None,
                      **kwargs):
    """
    Create a model card for a PAL algorithm.

    Parameters
    ----------
    model : object
        The PAL algorithm object.
    language : str, optional
        The language used to train the model.
    license : str, optional
        The license of the model.
    library_name : str, optional
        The library name of the model.
    tags : list, optional
        The tags of the model.
    base_model : str, optional
        The base model of the model.
    datasets : dict, optional
        The datasets used to train the model.
    metrics : dict, optional
        The metrics of the model.
    eval_results : dict, optional
        The evaluation results of the model.
    model_name : str, optional
        The name of the model.
    model_version : str, optional
        The version of the model.
    card_data : ModelCardData, optional
        The model card data.
    **kwargs : dict
        Additional keyword arguments.
    """
    model_info = _get_model_stats(model)
    training_data = model_info.get("training_data", None)
    training_data_signature = model_info.get("training_data_signature", None)
    initial_params = model.hanaml_parameters
    training_regime = model.hanaml_fit_params
    model_metrics = model_info.get("model_metrics", None)
    testing_data = model_info.get("testing_data", None)
    testing_data_signature = model_info.get("testing_data_signature", None)
    testing_metrics = model_info.get("testing_metrics", None)
    hours_used = model.runtime/3600
    #in case user overwrite the predefined elements
    if "training_data" in kwargs:
        training_data = kwargs.pop("training_data")
    if "training_data_signature" in kwargs:
        training_data_signature = kwargs.pop("training_data_signature")
    if "initial_params" in kwargs:
        initial_params = kwargs.pop("initial_params")
    if "training_regime" in kwargs:
        training_regime = kwargs.pop("training_regime")
    if "model_metrics" in kwargs:
        model_metrics = kwargs.pop("model_metrics")
    if "testing_data" in kwargs:
        testing_data = kwargs.pop("testing_data")
    if "testing_data_signature" in kwargs:
        testing_data_signature = kwargs.pop("testing_data_signature")
    if "testing_metrics" in kwargs:
        testing_metrics = kwargs.pop("testing_metrics")
    if "hours_used" in kwargs:
        hours_used = kwargs.pop("hours_used")
    if language is None:
        language = "en"
    if license is None:
        license = "SAP Developer License Agreement"
    if library_name is None:
        library_name = model.__module__ + '.' + type(model).__name__
    if datasets is None:
        datasets = {}
        if training_data:
            datasets["training_data"] = training_data
        if testing_data:
            datasets["testing_data"] = testing_data
        if training_data_signature:
            datasets["training_data_signature"] = training_data_signature
        if testing_data_signature:
            datasets["testing_data_signature"] = testing_data_signature
    if metrics is None:
        metrics = {}
        if model_metrics:
            metrics["model_metrics"] = model_metrics
        if testing_metrics:
            metrics["testing_metrics"] = testing_metrics
    if model_name is None:
        if hasattr(model, "name"):
            model_name = model.name
    if model_version is None:
        if hasattr(model, "version"):
            model_version = model.version
    if card_data is None:
        card_data = ModelCardData(
            model_name=model_name,
            model_version=model_version,
            language=language,
            license=license,
            library_name=library_name,
            tags=tags,
            datasets=datasets,
            metrics=metrics,
            eval_results=eval_results,
            base_model=base_model,
            **kwargs
        )
    card = ModelCard.from_template(card_data,
                                   template_path=os.path.join(os.path.dirname(__file__), "templates", "modelcard_template.md"),
                                   language=language,
                                   license=license,
                                   training_data=training_data,
                                   initial_params=initial_params,
                                   training_regime=training_regime,
                                   model_metrics=model_metrics,
                                   testing_data=testing_data,
                                   testing_metrics=testing_metrics,
                                   hours_used=hours_used,
                                   **kwargs)
    setattr(model, "model_card_", str(card))
    return card
