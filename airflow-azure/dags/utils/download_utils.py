import requests

claim_name = 'my-pvc'

def find_last_true_occurrence(bool_list):
    n = len(bool_list)
    low = 0
    high = n - 1
    last_true_index = -1

    while low <= high:
        mid = low + (high - low) // 2

        if (bool_list[mid])():
            last_true_index = mid
            low = mid + 1  # Adjust the search space to the right (since all subsequent elements are False)
        else:
            high = mid - 1

    return last_true_index

def lista_gen(url):
    def test_url():
        print(url)
        try:
            response = requests.get(url, timeout=2)
            return response.status_code == 200
        except:
            return False

    return test_url

def lista_gen_teste(url):
    return lambda: url < 5    