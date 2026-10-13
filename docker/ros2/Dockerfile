FROM ros:humble-ros-base

ENV DEBIAN_FRONTEND=noninteractive \
    ROS_DISTRO=humble \
    LIBGL_ALWAYS_SOFTWARE=1 \
    GALLIUM_DRIVER=llvmpipe \
    QT_QPA_PLATFORM=offscreen \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      ros-humble-gazebo-ros-pkgs \
      ros-humble-nav2-msgs \
      ros-humble-nav-msgs \
      ros-humble-geometry-msgs \
      python3-pip \
      git \
      curl \
      ca-certificates \
      xvfb \
      libgl1-mesa-dri \
      libglu1-mesa \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-27.5.1.tgz \
      | tar -xz -C /tmp \
    && mv /tmp/docker/docker /usr/local/bin/docker \
    && rm -rf /tmp/docker

RUN pip3 install --no-cache-dir \
      "fastapi>=0.115" "uvicorn[standard]>=0.32" "pydantic>=2.9" "pydantic-settings>=2.6" \
      "httpx>=0.27" "duckdb>=1.1" "redis>=5.2" "nats-py>=2.9" "loguru>=0.7" \
      "pymavlink>=2.4.40" "pyyaml>=6" "sqlalchemy>=2.0" "aiosqlite>=0.20" "alembic>=1.14" \
      "psycopg[binary]>=3.2" "numpy>=1.26" "pytest>=8.3" "websockets>=13" \
      "jinja2>=3.1" "python-multipart>=0.0.9"

COPY make_world.py waypoint_follower.py entrypoint.sh /opt/cnc/
RUN chmod +x /opt/cnc/entrypoint.sh /opt/cnc/make_world.py /opt/cnc/waypoint_follower.py

WORKDIR /work
ENTRYPOINT ["/opt/cnc/entrypoint.sh"]
