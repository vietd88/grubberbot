import unittest

import funcs_chesscom as fcc


class TestGameID(unittest.TestCase):
    def test_normal_url(self):
        game_id = "https://www.chess.com/game/live/28903829651"
        result = 28903829651
        self.assertEqual(fcc.game_id_from_url(game_id), result)


if __name__ == "__main__":
    unittest.main()
