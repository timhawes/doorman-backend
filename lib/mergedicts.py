import collections.abc


def mergedicts(dicts):
    """Recursively merges a series of dicts. Earlier dicts get priority."""
    output = {}
    for d in dicts:
        for k, v in d.items():
            if k not in output:
                # print(f"{k} set to {v}")
                output[k] = v
            else:
                if isinstance(v, collections.abc.Mapping) and isinstance(
                    output[k], collections.abc.Mapping
                ):
                    # print(f"{k} recursively merging {v}")
                    output[k] = mergedicts([output[k], v])
                # else:
                # print(f"not merging {v} with {output[k]}")
    return output


def get(data, key, parent_key):
    this = data[key]
    if parent_key in this:
        parents = []
        if type(this[parent_key]) is list:
            for parent_name in this[parent_key]:
                parents.append(get(data, parent_name, parent_key))
        else:
            parent_name = this[parent_key]
            parents.append(get(data, parent_name, parent_key))
        output = mergedicts([this] + parents)
        del output[parent_key]
        return output
    else:
        return this


def tests():
    a = {
        "data": {
            "list1": [1, 2, 3, 4],
            "dict1": {"foo": "bar"},
            "value1": 123,
            "value2": "abc",
        }
    }
    b = {
        "_parents": ["a"],
        "toplevel1": "xyz",
        "data": {"dict1": {"foo": "BAR", "val1": "baz"}},
    }
    c = {"_parents": ["b"], "toplevel1": "def", "data": {"dict1": {"val1": "Baz"}}}
    big = {"a": a, "b": b, "c": c}

    output1 = {
        "_parents": ["a"],
        "data": {
            "list1": [1, 2, 3, 4],
            "dict1": {"foo": "bar", "val1": "baz"},
            "value1": 123,
            "value2": "abc",
        },
        "toplevel1": "xyz",
    }
    assert mergedicts([a, b, c]) == output1

    output2 = {
        "_parents": ["b"],
        "toplevel1": "def",
        "data": {
            "list1": [1, 2, 3, 4],
            "dict1": {"foo": "BAR", "val1": "Baz"},
            "value1": 123,
            "value2": "abc",
        },
    }
    assert mergedicts([c, b, a]) == output2

    print(get(big, "a", "_parents"))
    print(get(big, "b", "_parents"))
    print(get(big, "c", "_parents"))
    del output2["_parents"]

    print(output2)
    assert get(big, "c", "_parents") == output2


if __name__ == "__main__":
    tests()
    print("OK")
