#!/usr/bin/env python3

import unittest
from ci_config import CI


class TestCIConfig(unittest.TestCase):
    def test_runner_config(self):
        """check runner is provided w/o exception"""
        for job in CI.JobNames:
            self.assertIn(CI.JOB_CONFIGS[job].runner_type, CI.Runners)

    def test_job_stage_config(self):
        """
        check runner is provided w/o exception
        """
        # check stages
        for job in CI.JobNames:
            if job in CI.BuildNames:
                self.assertTrue(
                    CI.get_job_ci_stage(job)
                    in (CI.WorkflowStages.BUILDS_1, CI.WorkflowStages.BUILDS_2)
                )
            else:
                if job in (
                    CI.JobNames.STYLE_CHECK,
                    CI.JobNames.FAST_TEST,
                    CI.JobNames.JEPSEN_SERVER,
                    CI.JobNames.JEPSEN_KEEPER,
                    CI.JobNames.BUILD_CHECK,
                ):
                    self.assertEquals(
                        CI.get_job_ci_stage(job),
                        CI.WorkflowStages.NA,
                        msg=f"Stage for [{job}] is not correct",
                    )
                else:
                    self.assertTrue(
                        CI.get_job_ci_stage(job)
                        in (CI.WorkflowStages.TESTS_1, CI.WorkflowStages.TESTS_3),
                        msg=f"Stage for [{job}] is not correct",
                    )

    def test_build_jobs_configs(self):
        """
        check build jobs have non-None build_config attribute
        check test jobs have None build_config attribute
        """
        for job in CI.JobNames:
            if job in CI.BuildNames:
                self.assertTrue(
                    isinstance(CI.JOB_CONFIGS[job].build_config, CI.BuildConfig)
                )
            else:
                self.assertTrue(CI.JOB_CONFIGS[job].build_config is None)
