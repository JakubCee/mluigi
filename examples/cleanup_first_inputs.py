"""
Testing move and rename in flow of local targets.
Could be used for VMO
- user inputs files into ROOT
- luigi will scan folder every day (simulated as task GenerateFile)
- if new files found, parse content, execute procedure for PE Exception insert (task ProcessFiles)
- processed files resp its paths will be put into other file: processed_datetime.txt
- cleanup task will open processed_datetime.txt, read filepaths and remove these files.


Make sure that:
* Outputs follow timestamp filename conventions
* root is specified
* custom `self.complete` method is specified to ensure completeness of the flow


Tasks:
* GenerateFile
    - Generates the files, these will be deleted by task RemoveFiles
* ProcessFile
    - do some processing, reads the inputs and outputs filepaths of the inputs
    - content of output (paths of input - from task GenerateFile) goes to RemoveFiles task
    - output of this task should prove completion of the processing.
* RemoveFiles
    - reads output of ProcessFile (paths of very first inputs)
    - each path read is removed
    - custom `self.complete` method must take place to ensure that files are deleted.

"""
import luigi
from pathlib import Path
import logging
from datetime import datetime


logger = logging.getLogger(__name__)
root = Path().cwd()

RUNTIME = datetime.now().strftime("%Y-%m-%d_%H%M%S")


class GenerateFile(luigi.Task):
    file_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(path=self.file_path)

    def run(self):
        with self.output().open('w') as f:
            f.write(str(self.file_path))


class ProcessFile(luigi.Task):
    def requires(self):
        return [GenerateFile(file_path=root/f"file_{i}.txt") for i in range(3)]

    def run(self):
        contents = []
        for i in self.input():
            with i.open('r') as f:
                content = f.read()
            print(f"******{i.path} is processed")
            contents.append(content)

        with self.output().open('w') as of:
            for line in contents:
                of.write(f"{line}\n")

    def output(self):
        return luigi.LocalTarget(root / "processed" / f"processed_{RUNTIME}.txt")


class RemoveFiles(luigi.Task):
    def requires(self):
        return ProcessFile()

    def run(self):
        with self.input().open('r') as f:
            for line in f:
                Path(line.strip()).unlink(missing_ok=True)

    def complete(self):
        try:
            completed = []
            with self.input().open('r') as f:
                for line in f:
                    completed.append(not Path(line.strip()).exists())
            return all(completed)
        except FileNotFoundError as e:
            return False



if __name__ == '__main__':
    from pathlib import Path
    t = RemoveFiles()
    luigi.build([t], local_scheduler=True)



