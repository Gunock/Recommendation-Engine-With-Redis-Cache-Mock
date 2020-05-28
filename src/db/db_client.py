from abc import abstractmethod


class DbClient:
    @abstractmethod
    def profile_exists(self, user_id: int) -> bool:
        pass

    @abstractmethod
    def get_profile(self, user_id: int) -> dict:
        pass

    @abstractmethod
    def add_profile(self, user_id: int, profile_json: dict) -> None:
        pass

    @abstractmethod
    def remove_profile(self, user_id: int) -> dict:
        pass
