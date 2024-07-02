from datatrove.executor.base import PipelineExecutor
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.dedup import (
    SentenceDedupFilter,
    SentenceDedupSignature,
    SentenceFindDedups,
)
from datatrove.pipeline.dedup.sentence_dedup import SentDedupConfig
from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.pipeline.tokens import TokensCounter



"""
example on how to use sentence-deduplication. sentence-deduplication implements deduplication as in:
https://jmlr.org/papers/v21/20-074.html
    'To deduplicate the data set, we discarded all but one of any three-sentence span
    occurring more than once in the data set.'

to run deduplication we need to run three different pipelines,
pipeline 1:
    implements usual extraction + quality filtering, it ends with SentenceDedupSignature, preprended by a writer.
pipeline 2:
    implements only SentenceFindDedups
pipeline 3:
    implements SentenceDedupFilter prepended by a reader of the same writer-kind used during stage 1. after the
    SentenceDedupFilter.
"""

PART = "sentence-deduped"
S3_FILTERED_PATH = f""
S3_LOGS_FOLDER = f"{S3_FILTERED_PATH}/logs/{PART}"
S3_DATA_PATH = f""


TOTAL_TASKS = 50


INPUT_READER = JsonlReader(
    data_folder=S3_DATA_PATH,
    recursive=True,
    file_progress=True,
)

# modify sentence dedup hyper params here
sent_dedup_config = SentDedupConfig(
    n_sentences=3,
    split_sentences=False,  # set to False to split on \n instead
    only_dedup_in_index=True,
    min_doc_words=50,
)

FINDER_WORKERS = 50  # this will speed up/parallelize step 2


def run_pipeline():
    pipeline_1 = [
        INPUT_READER,
        SentenceDedupSignature(
            output_folder=f"{S3_FILTERED_PATH}/{PART}/sigs",
            config=sent_dedup_config,
            finder_workers=FINDER_WORKERS,
        ),
    ]

    pipeline_2 = [
        SentenceFindDedups(
            data_folder=f"{S3_FILTERED_PATH}/{PART}/sigs",
            output_folder=f"{S3_FILTERED_PATH}/{PART}/dups",
            config=sent_dedup_config,
        )
    ]

    pipeline_3 = [
        INPUT_READER,
        SentenceDedupFilter(
            data_folder=f"{S3_FILTERED_PATH}/{PART}/dups",
            config=sent_dedup_config,
            exclusion_writer=JsonlWriter(output_folder=f"{S3_FILTERED_PATH}/{PART}/removed"),
        ),
        TokensCounter(tokenizer_name_or_path="aubmindlab/aragpt2-base"),
        JsonlWriter(output_folder=f"{S3_FILTERED_PATH}/{PART}/output"),
    ]

    executor_1: PipelineExecutor = LocalPipelineExecutor(
        pipeline=pipeline_1, workers=50, tasks=50, logging_dir=f"{S3_LOGS_FOLDER}/sigs"
    )

    executor_2: PipelineExecutor = LocalPipelineExecutor(
        pipeline=pipeline_2,
        workers=1,
        tasks=FINDER_WORKERS,
        logging_dir=f"{S3_LOGS_FOLDER}/dups",
    )

    executor_3: PipelineExecutor = LocalPipelineExecutor(
        pipeline=pipeline_3,
        workers=50,
        tasks=50,
        logging_dir=f"{S3_LOGS_FOLDER}/filter",
    )

    print(executor_1.run())
    print(executor_2.run())
    print(executor_3.run())


if __name__ == "__main__":
    run_pipeline()
