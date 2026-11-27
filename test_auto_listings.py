import unittest

from auto_listings import matches_auto, parse_sales, read_specs


class AutoListingsTest(unittest.TestCase):
    def test_model_page_accepts_two_liters_and_ignores_old_mileage_filter(self):
        body = '''<tr id="tr_1"><td><a class="am" href="/msg/lv/transport/cars/volkswagen/tiguan/abc.html">Auto</a></td>
          <td class="pp6">2019</td><td class="pp6">2.0D</td><td class="pp6">-</td>
          <td class="pp6">14,000 €</td></tr>'''
        car, = parse_sales(body, 'volkswagen', 'tiguan')
        self.assertTrue(matches_auto(car, {'model_slug': 'tiguan', 'min_liters': 1, 'max_liters': 2,
                                           'min_mileage': 120000, 'max_mileage': 160000}))
        self.assertFalse(matches_auto(car, {'max_liters': 1.9}))
        self.assertFalse(matches_auto(car, {'model_slug': 'golf-7'}))

    def test_sale_rows_keep_only_priced_passenger_cars(self):
        url = '/msg/lv/transport/cars/bmw/530/abc.html'
        row = '''<tr id="tr_123"><td><a class="am" href="{}">Auto</a></td>
          <td class="msga2-o pp6">530</td><td class="msga2-o pp6">2019</td>
          <td class="msga2-o pp6">3.0D</td><td class="msga2-r pp6">150 tūkst.</td>
          <td class="msga2-o pp6">{}</td></tr>'''
        cars = list(parse_sales(row.format(url, '23,000 €') + row.format(url, 'pērku'), 'bmw'))
        self.assertEqual(len(cars), 1)
        self.assertEqual(cars[0]['price'], 23000)
        self.assertEqual(cars[0]['auto_data']['mileage'], 150000)
        self.assertTrue(matches_auto(cars[0], {'model': '530', 'min_liters': 2.5, 'max_year': 2019}))
        self.assertFalse(matches_auto(cars[0], {'model': '53'}))

    def test_engine_and_gearbox_match_independently_of_displacement(self):
        specs = read_specs('<td id="tdo_15">3.0 dīzelis</td><td id="tdo_35">Automāts 8 ātrumi</td>')
        listing = {'auto_data': {'model': '530', 'model_slug': '530', 'liters': 3.0}}
        self.assertTrue(matches_auto(listing, {'engine': 'diesel', 'transmission': 'automatic'}, specs))
        self.assertFalse(matches_auto(listing, {'engine': 'petrol', 'transmission': 'automatic'}, specs))
        self.assertFalse(matches_auto(listing, {'transmission': 'manual'}, specs))
