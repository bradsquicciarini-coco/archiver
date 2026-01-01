# write out camera info topics and static tfs


from pathlib import Path
from typing import Literal

import foxglove
import yaml
import numpy as np
from scipy.spatial.transform import Rotation

from foxglove.channels import FrameTransformsChannel, CameraCalibrationChannel
from foxglove.schemas import FrameTransform, FrameTransforms, Timestamp, Vector3, Quaternion, CameraCalibration

INPUT_H, INPUT_W = 1080, 1920
FOCAL = 790
# K_d = np.array([[FOCAL, 0, INPUT_W / 2.0], [0, FOCAL, INPUT_H / 2.0], [0, 0, 1]])
K_d = np.array([[807.327, 0, 978.781], [0, 807.327, 576.948], [0, 0, 1]])
D = np.array([-0.026, 1.859e-3, 1.131e-4, 7.901e-5])

# Hardcoded initial guesses
T_cam2lidar = np.eye(4)
# T_cam2lidar[:3, 3] = np.array([0.000, -0.118, -0.012])
# T_cam2lidar[:3, :3] = Rotation.from_euler("xyz", [0.503, -72.812, 90.723], degrees=True).as_matrix()
T_cam2lidar[:3, 3] = np.array([0.000, -0.118, -0.012])
T_cam2lidar[:3, :3] = Rotation.from_euler("xyz", [0.822, -75.315, 88.748], degrees=True).as_matrix()

T_baselink2cam = np.eye(4)
T_baselink2cam[:3, 3] = np.array([0.382, 0.000, 0.000])
T_baselink2cam[:3, :3] = Rotation.from_euler("xyz", [-90.000, -0.000, -90.000], degrees=True).as_matrix()


def to_timestamp(t_sec_float: float) -> Timestamp:
    secs = int(t_sec_float)
    nsecs = int((t_sec_float - secs) * 1e9)
    return Timestamp(secs, nsecs)


def matrix_to_tq(T: np.ndarray):
    x, y, z = T[:3, 3]
    t = Vector3(x=x, y=y, z=z)
    qx, qy, qz, qw = Rotation.from_matrix(T[:3, :3]).as_quat()
    q = Quaternion(x=qx, y=qy, z=qz, w=qw)
    return t, q


def make_4x4_from_qt(q: np.ndarray, t: np.ndarray):
    T = np.eye(4)
    T[:3, :3] = Rotation.from_quat(q).as_matrix()
    T[:3, 3] = t
    return T


def write_out_camera_calibration(output_fp: str, camera_version: Literal["1"] | Literal["1.5-1"] | Literal["1.5-3"], start_ts: float):

    HERE = Path(__file__).resolve().parent
    profile_path = HERE / "cam_profiles.yml"
    with profile_path.open() as f:
        data = yaml.safe_load(f)
        instrinsics = data["cameras"]
        transforms = data["transforms"]

    ts_ns = int(start_ts * 1e9)

    with foxglove.open_mcap(output_fp, allow_overwrite=True):
        # log transforms
        chan_tf = FrameTransformsChannel(topic="/tf_static")
        tfs_to_log = [(tf["parent"], tf["child"], make_4x4_from_qt(tf["rotation"], tf["translation"])) for tf in transforms]
        frame_transforms = []
        for parent, child, T in tfs_to_log:
            translation, rotation = matrix_to_tq(T)
            frame_transforms.append(
                FrameTransform(
                    timestamp=to_timestamp(ts_ns / 1e9),
                    parent_frame_id=parent,
                    child_frame_id=child,
                    translation=translation,
                    rotation=rotation,
                )
            )
        chan_tf.log(FrameTransforms(transforms=frame_transforms), log_time=ts_ns)

        # calibration
        for cam_name, data in instrinsics.items():

            frame_id = data["frame_id"]
            chan_cam_calib = CameraCalibrationChannel(topic=f"/{frame_id}/camera_info")

            # NOTE(Brad): hack
            R = np.eye(3).flatten().tolist()
            if camera_version == "1.5-1" and cam_name in ("left", "right"):
                data = instrinsics["back"]
            elif camera_version == "1":
                data = instrinsics["back"]
                if cam_name == "front":
                    R = Rotation.from_euler("xyz", [0, 0, 90], degrees=True).as_matrix().flatten().tolist()

            i = data["intrinsics"]
            fx, fy, cx, cy = i["fx"], i["fy"], i["cx"], i["cy"]
            K = np.array([[fx, 0, cx], [0, fy, cy], [0, 0, 1]])
            calibration = CameraCalibration(
                timestamp=Timestamp(sec=int(ts_ns / 1e9), nsec=int(ts_ns % 1_000_000_000)),
                frame_id=frame_id,
                width=data["resolution"]["w"],
                height=data["resolution"]["h"],
                distortion_model=data["distortion_model"],
                D=data["D"],
                K=K.flatten().tolist(),
                P=np.hstack([K, np.zeros((3, 1))]).flatten().tolist(),
                R=R,
            )
            chan_cam_calib.log(calibration, log_time=ts_ns)
