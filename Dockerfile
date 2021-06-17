FROM python:3.8
ARG TZ_AREA=Australia
ARG TZ_ZONE=Sydney
ARG IMAGE_NAME_DIR

ENV dagr.plugins.selenium.enabled=TRUE
ENV dagr.plugins.selenium.webdriver_mode=remote
ENV dagr.plugins.selenium.webdriver_url="http://chrome:4444/wd/hub"
ENV dagr.plugins.classes.browser=selenium
ENV dagr.plugins.classes.crawler=selenium

ENV PATH="/home/dagr/.local/bin:${PATH}"


RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
		tk-dev \
		uuid-dev \
	&& bash -c "echo '$TZ_AREA/$TZ_ZONE' > /etc/timezone" \
	&& unlink /etc/localtime \
	&& dpkg-reconfigure -f noninteractive tzdata \
	&& useradd dagr 

WORKDIR /dagr_selenium

COPY . .

RUN pip install .[full] && mkdir -v /output /DA /home/dagr \
 && chown -Rv dagr:dagr /output /home/dagr /DA \
 && chmod a+rw -Rv /output /DA

USER dagr


WORKDIR /output

ENTRYPOINT ["python3"]