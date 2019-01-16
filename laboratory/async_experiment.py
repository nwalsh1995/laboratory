from laboratory.experiment import Experiment
from laboratory import exceptions
from laboratory.result import Result

from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from threading import Thread
import logging


logger = logging.getLogger(__name__)


class CandidateManager(Thread):

    def __init__(self, experiment, control_observation, candidate_functions, max_child_threads=2, timeout=60):
        super().__init__()

        self.control_observation = control_observation
        self.candidate_functions = candidate_functions
        self.timeout = timeout

        self.experiment = experiment

        self.thread_pool = ThreadPoolExecutor(max_workers=max_child_threads)

    def run(self):
        candidate_observations = []
        future_to_candidate = {}
        for candidate_function in self.candidate_functions:
            future = self.thread_pool.submit(candidate_function)
            future_to_candidate[future] = candidate_function

        for future in as_completed(future_to_candidate):
            try:
                observation, is_control = future.result(timeout=self.timeout)
            except TimeoutError:
                continue

            candidate_observations.append(observation)

        result = Result(self.experiment, self.control_observation, candidate_observations)

        try:
            self.experiment.publish(result)
        except Exception:
            msg = 'Exception occured when publishing %s experiment data'
            logger.exception(msg % self.name)


class AsyncExperiment(Experiment):
    """Returns the control's result as soon as possible while the candidate functions are running in the background.

    Only publishes once all the async functions have finished executing or have been killed.
    """

    def conduct(self, randomize=False):
        if self._control is None:
            raise exceptions.LaboratoryException(
                'Your experiment must contain a control case'
            )

        # execute control and exit if experiment is not enabled
        if not self.enabled():
            control = self._run_tested_func(raise_on_exception=True, **self._control)
            return control.value

        def get_func_executor(obs_def, is_control):
            """A lightweight wrapper around a tested function in order to retrieve state"""
            return lambda *a, **kw: (self._run_tested_func(raise_on_exception=is_control, **obs_def), is_control)

        control_observation, is_control = get_func_executor(self._control, is_control=True)()
        candidate_functions = [get_func_executor(cand, is_control=False) for cand in self._candidates]
        candidate_manager = CandidateManager(self, control_observation, candidate_functions)

        try:
            candidate_manager.start()
        except Exception:
            logger.exception("Failed to run candidate manager thread")

        return control_observation.value
