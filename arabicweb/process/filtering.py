import sys
import os


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datatrove.executor.base import PipelineExecutor
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.writers.jsonl import JsonlWriter
from pipelines.filters import FineWebQualityFilter, C4BadWordsFilter
from datatrove.pipeline.formatters import SymbolLinesFormatter
from pipelines.formatters import ImageHtmlRemover



PART = "formatted-filtered"
S3_FILTERED_PATH = f""
S3_LOGS_FOLDER = f"{S3_FILTERED_PATH}/logs/{PART}"
S3_DATA_PATH = f""


INPUT_READER = JsonlReader(
    data_folder=S3_DATA_PATH,
    recursive=True,
    file_progress=True,
)


# set the local path to the dir of the custom banned words for filtering


def run_pipeline():
    pipeline = [
        INPUT_READER,
        C4BadWordsFilter(
            local_path="",
            exclusion_writer=JsonlWriter(output_folder=f"{S3_FILTERED_PATH}/{PART}/removed-badwords"),
        ),
        FineWebQualityFilter(exclusion_writer=JsonlWriter(output_folder=f"{S3_FILTERED_PATH}/{PART}/removed-fineweb")),
        ImageHtmlRemover(),
        SymbolLinesFormatter(),
        JsonlWriter(output_folder=f"{S3_FILTERED_PATH}/{PART}/output"),
    ]

    executor: PipelineExecutor = LocalPipelineExecutor(
        pipeline=pipeline,
        workers=50,
        tasks=50,
        logging_dir=f"{S3_LOGS_FOLDER}/formatted-filtered",
    )

    print(executor.run())


if __name__ == "__main__":
    run_pipeline()
