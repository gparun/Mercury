from app import Results, ActionStatus, AppException


class DictUtils:
    @staticmethod
    def remove_empty_strings(dict_to_clean: dict) -> Results:
        """
        Removes all the empty key+value pairs from the dict you give it; use to clean up dicts before persisting them to the DynamoDB
        :param dict_to_clean: as dict()
        :return: only non-empty key+value pairs from the source dict as dict() inside Results
        """
        try:
            # start with your guard clauses - thats a standard situation you have to handle in your error handling
            assert type(dict_to_clean) is dict

            # here comes processing...
            def recursive_clean(dict_to_process: dict) -> dict:
                for key in list(dict_to_process.keys()):
                    value = dict_to_process[key]
                    # clean if empty string or collection or None
                    if isinstance(value, (str, dict, list, tuple, type(None))) and not value:
                        del dict_to_process[key]
                    elif type(value) is dict:
                        processed_dict = recursive_clean(value)
                        if processed_dict:
                            dict_to_process[key] = processed_dict
                        else:
                            del dict_to_process[key]

                return dict_to_process

            cleaned_dict = recursive_clean(dict_to_clean)

            # now you are ready to ship back...
            output = Results()
            output.ActionStatus = ActionStatus.SUCCESS
            output.Results = cleaned_dict
            return output

        # here we have exceptions we KNOW we can encounter...
        except AssertionError:
            output = Results()
            output.Results = "You have to pass dict to this method!"
            return output

        # and this analog of *nix panic(*message)...
        except Exception as e:
            catastrophic_exception = AppException(ex=e, message='Catastrophic failure when trying to clean up dict '
                                                                'from the Dynamo!')
            raise catastrophic_exception
