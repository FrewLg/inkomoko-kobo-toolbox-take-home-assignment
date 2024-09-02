from koboextractor import KoboExtractor
kobo = KoboExtractor('Token f24b97a52f76779e97b0c10f80406af5e9590eaf' , 'https://kf.kobotoolbox.org/api/v2/assets/aW9w8jHjn4Cj8SSQ5VcojK/data.json', debug=debug)

assets = kobo.list_assets()
asset_uid = assets['results'][0]['uid']


asset = kobo.get_asset(asset_uid)
choice_lists = kobo.get_choices(asset)
questions = kobo.get_questions(asset=asset, unpack_multiples=True)

print(questions)

new_data = kobo.get_data(asset_uid, submitted_after='2020-05-20T17:29:30')

new_results = kobo.sort_results_by_time(new_data['results'])


labeled_results = []
for result in new_results: # new_results is a list of list of dicts
        # Unpack answers to select_multiple questions
        labeled_results.append(kobo.label_result(unlabeled_result=result, choice_lists=choice_lists, questions=questions, unpack_multiples=True))
