import pytest

from durable_queue.registry import get_task, task


def test_task_registers_by_function_name():
    @task
    def registry_sample_task() -> None:
        pass

    assert get_task("registry_sample_task") is registry_sample_task


def test_task_raises_on_duplicate_registration():
    @task
    def registry_dup_task() -> None:
        pass

    with pytest.raises(ValueError):

        @task
        def registry_dup_task() -> None:  # noqa: F811
            pass


def test_get_task_raises_on_unknown_name():
    with pytest.raises(KeyError):
        get_task("does_not_exist_task_xyz")
