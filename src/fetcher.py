import hashlib


class NotFound(Exception):
    pass


class StageSelector:
    def __init__(self, staged_element_name):
        self.staged_element_name = staged_element_name

    def __call__(self, documents):
        if len(documents) == 0:
            raise NotFound(str.format(
                "StageSelector: there should be at least one current document for each level in the hierarchy"))
        name = documents[0]["name"]
        if name == self.staged_element_name:
            selected = [document for document in documents if document["state"] == "stage"]
            if len(selected) != 1:
                raise NotFound(str.format("StageSelector: staged element not found, name {}", self.staged_element_name))
        else:
            selected = [document for document in documents if document["state"] == "current"]
            if len(selected) != 1:
                raise NotFound(str.format("StageSelector: there should be at least one current document for each level in the hierarchy"))

        return selected[0]


def consistent_hash(s):
    md5 = hashlib.md5(s.encode("utf-8"))
    return int(md5.hexdigest(), 16)


class RolloutSelector:
    def __init__(self, consumer_key):
        self.consumer_key = consumer_key

    def __call__(self, documents):
        current = [document for document in documents if document["state"] == "current"]
        rollout = [document for document in documents if document["state"] == "rollout"]
        if len(current) != 1:
            raise NotFound(str.format(
                "RolloutSelector: there should be at least one current document for each level in the hierarchy"))
        if len(rollout) == 1:
            # TODO: unit test
            element = rollout[0]
            percentile = consistent_hash(self.consumer_key) % 10000 + 1
            rollout_threshold = int(element["population"] * 100)
            if percentile <= rollout_threshold:
                return element
            else:
                return current[0]
        else:
            return current[0]


class CurrentSelector:
    def __init__(self):
        pass

    def __call__(self, documents):
        if len(documents) == 0:
            raise NotFound(
                "CurrentSelector: there should be at least one current document for each level in the hierarchy")
        selected = [document for document in documents if document["state"] == "current"]
        if len(selected) != 1:
            raise Exception(
                "CurrentSelector: there should be only one current document for each level in the hierarchy")
        return selected[0]


def fetch_hierarchy(select, configurations_collection, application_name, element_name, element_type, fallback_names=None):
    # TODO  add per revision hash on each edit and return a hash of hashes with resolved document
    result = list()
    try:
        document_cursor = configurations_collection.find(
            {
                "name": element_name, 
                "type": element_type, 
                "application": application_name
            })
        documents = [document for document in document_cursor]
        while not documents and fallback_names:
            el_name = fallback_names.pop(0)
            document_cursor = configurations_collection.find({
                "name": el_name,
                "type": element_type,
                "application": application_name
            })
            documents = [document for document in document_cursor]
        if not documents:
            return []
        document = select(documents)
        result.append(document)
        while document["defaults"] != "":
            document_cursor = configurations_collection.find(
                {
                    "name": document["defaults"], 
                    "type": element_type, 
                    "application": application_name
                })
            documents = [document for document in document_cursor]
            document = select(documents)
            result.append(document)

        result.reverse()
    except Exception as e:
        raise Exception(
            str.format(
                "query for element {} of type {} in application {} raised and exception {}",
                element_name,
                element_type,
                application_name,
                e
            )
        )
    return result
