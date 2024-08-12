"""
AHItoDICOM Module : This class contains the logic to query the Image pixel raster.

SPDX-License-Identifier: Apache-2.0
"""
import logging
from openjpeg.utils import decode
import io


class AHIFrameFetcher:

    def __init__(self, ahi_client):
        self.logger = logging.getLogger(__name__)
        self.ahi_client = ahi_client

    def fetch(self, frame):

        pixel_data = b""
        has_data = False

        for frame_id in frame["frameIds"]:
            self.logger.info(f"Fetching {frame_id}")

            try:
                frame["PixelData"] += self.get_frame_pixels(
                    frame["datastoreId"], frame["imagesetId"], frame_id)
                has_data = True
            except TypeError:
                self.logger.warning(
                    f"No pixel data for {frame['datastoreId'] = }, {frame['imagesetId'] = } {frame_id = }")
                continue

        if has_data:
            frame["PixelData"] = pixel_data

        return frame


    def get_frame_pixels(self, datastore_id, imageset_id, image_frame_id) -> bytes | None:
        try:
            res = self.ahi_client.get_image_frame(
                datastoreId=datastore_id,
                imageSetId=imageset_id,
                imageFrameInformation={"imageFrameId": image_frame_id})
            b = io.BytesIO(res["imageFrameBlob"].read())
            b.seek(0)

            return decode(b).tobytes()

        except Exception:
            self.logger.exception("Frame could not be decoded")
