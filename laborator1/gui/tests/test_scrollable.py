from tkinter import ttk

from scrollable import ScrollableFrame
from support import TkTestCase, scroll_height


class ScrollableFrameTest(TkTestCase):
    def make(self, rows):
        frame = ScrollableFrame(self.root, height=100)
        frame.pack()
        for _ in range(rows):
            ttk.Label(frame.body, text="row").pack()
        frame.refresh()
        return frame

    def test_scroll_region_covers_content_taller_than_the_view(self):
        frame = self.make(40)
        self.assertGreaterEqual(scroll_height(frame), frame.body.winfo_reqheight())
        self.assertGreater(scroll_height(frame), 100)

    def test_scroll_region_grows_after_more_content_and_refresh(self):
        frame = self.make(5)
        before = scroll_height(frame)
        for _ in range(30):
            ttk.Label(frame.body, text="more").pack()
        frame.refresh()
        self.assertGreater(scroll_height(frame), before)

    def test_scroll_region_shrinks_after_content_is_removed(self):
        frame = self.make(30)
        for child in frame.body.winfo_children()[5:]:
            child.destroy()
        frame.refresh()
        self.assertLess(scroll_height(frame), 200)

    def test_body_lives_inside_the_canvas(self):
        frame = self.make(1)
        self.assertEqual(str(frame.body.master), str(frame.canvas))
