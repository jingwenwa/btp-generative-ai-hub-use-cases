"""
Dataset cards creation for hana-ml DataFrame.

The following function is available:

    :func:`create_dataset_card`
"""
#pylint: disable=protected-access, redefined-builtin
import logging
import os
from hana_ml.dataframe import DataFrame
try:
    from huggingface_hub import DatasetCard, DatasetCardData, RepoCard
except ImportError as error:
    logging.getLogger(__name__).error("%s: %s", error.__class__.__name__, str(error))
    pass

def _get_data_info(obj):
    data_info = {}
    if hasattr(obj, 'select_statement'):
        data_info['select_statement'] = obj.select_statement
    data_info['size_categories'] = 'other'
    data_info['dtypes'] = str({x[0]:x[1] for x in obj.dtypes()})
    data_info['name'] = obj.name
    data_info['shape'] = str(obj.shape)
    data_size = obj.count()
    if data_size < 1e3:
        size_category = 'n<1K'
    elif data_size < 1e4:
        size_category = '1K<n<10K'
    elif data_size < 1e5:
        size_category = '10K<n<100K'
    elif data_size < 1e6:
        size_category = '100K<n<1M'
    elif data_size < 1e7:
        size_category = '1M<n<10M'
    elif data_size < 1e8:
        size_category = '10M<n<100M'
    elif data_size < 1e9:
        size_category = '100M<n<1B'
    elif data_size < 1e10:
        size_category = '10B<n<100B'
    elif data_size < 1e11:
        size_category = '100B<n<1T'
    else:
        size_category = 'n>1T'
    data_info['size_categories'] = size_category
    return data_info

def parse_dataset_card(dataset_card):
    """
    Parse a model card.

    Parameters
    ----------
    model_card : str
        The model card markdown.
    """
    return RepoCard(dataset_card)

def create_dataset_card(data,
                        key=None,
                        language=None,
                        license=None,
                        annotations_creators=None,
                        language_creators=None,
                        multilinguality=None,
                        source_datasets=None,
                        task_categories=None,
                        pretty_name=None,
                        card_data=None,
                        **kwargs):
    r"""
    Create a dataset card for an SAP HANA DataFrame.

    Parameters
    ----------
    data : DataFrame
        DataFrame contains the dataset for creating the dataset card.

    language : str or ListOfStrings, optional
        Language of dataset's data or metadata. It must be an ISO 639-1, 639-2 or
        639-3 code (two/three letters), or a special value like "code", "multilingual".

    license : str or ListOfStrings, optional
        License(s) of this dataset.

    annotations_creators : str or ListOfStrings, optional
        How the annotations for the dataset were created.

        Valid options are: 'found', 'crowdsourced', 'expert-generated', 'machine-generated', 'no-annotation', 'other'.

    language_creators : str or ListOfStrings, optional
        How the text-based data in the dataset was created.
        Options are: 'found', 'crowdsourced', 'expert-generated', 'machine-generated', 'other'.

    multilinguality : str or ListOfStrings, optional
        Whether the dataset is multilingual.

        Options are: 'monolingual', 'multilingual', 'translation', 'other'.

    source_datasets : {'original', 'extended'}, optional
        Indicates whether the dataset is an original dataset or extended from another existing dataset.

    task_categories : str or ListOfStrings, optional
       What categories of task does the dataset support?

       Optionas are : 'classification', 'regression', 'time-series' and 'other'.

    pretty_name : str, optional
        A more human-readable name for the dataset. (e.g. "CoffeeAndTea").
    """
    msg = "Input data for dataset card generation should be a hana-ml DataFrame."
    assert isinstance(data, DataFrame), msg
    data_info = _get_data_info(data)
    if key is not None:
        data_info['key'] = key
    kwarg_input = {**kwargs}
    for arg in kwarg_input:
        if arg in data_info:#user-specified values take precedence over auto-generated ones
            del data_info[arg]
    if language is None:
        language = "en"
    if license is None:
        license = "SAP Developer License Agreement"
    if card_data is None:
        card_data = DatasetCardData(
            language=language,
            license=license,
            annotations_creators=annotations_creators,
            language_creators=language_creators,
            multilinguality=multilinguality,
            source_datasets=source_datasets,
            task_categories=task_categories,
            pretty_name=pretty_name,
            **data_info,
            **kwargs)
    card = DatasetCard.from_template(card_data=card_data,
                                     template_path=os.path.join(os.path.dirname(__file__), "templates", "datasetcard_template.md"),
                                     language=language,
                                     license=license,
                                     annotations_creators=annotations_creators,
                                     language_creators=language_creators,
                                     multilinguality=multilinguality,
                                     source_datasets=source_datasets,
                                     task_categories=task_categories,
                                     pretty_name=pretty_name,
                                     **data_info,
                                     **kwargs)
    setattr(data, "dataset_card_", str(card))
    return card
