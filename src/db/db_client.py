from abc import abstractmethod


class DbClient:
    @abstractmethod
    def profile_exists(self, user_id: int) -> bool:
        """
        Check if profile exists in database.

        :param user_id: userId of profile to be checked
        :return True if profile exists in database, otherwise False
        """
        pass

    @abstractmethod
    def get_profile(self, user_id: int) -> dict:
        """
        Retrieves profile from database.

        :param user_id: userId of profile to be retrieved
        :return profile if exists, otherwise empty dict
        """
        pass

    @abstractmethod
    def add_profile(self, user_id: int, profile_json: dict) -> None:
        """
        Save profile in database.

        :param user_id: userId of profile to be saved
        :param profile_json: profile details
        :return None
        """
        pass

    @abstractmethod
    def remove_profile(self, user_id: int) -> None:
        """
        Removes profile from database.

        :param user_id:
        :return None
        """
        pass
