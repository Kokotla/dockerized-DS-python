INSTRUCTION TO RUN THE CODE.
.....................................................................
.....................................................................

There are two codes provided. One to run on a mounted docker image. 

For file project_737491_d.py:
1. This code is ideal to run as a docker image.
2. This will take 3 arguments in this specific order:
	
	latitude longitude distance_range in km

	e.g  docker run -v /home/kokotla/Documents/Large_scale/project/simulation/data:/data -v /home/kokotla/Documents/Large_scale/project/simulation/output:/output kokotla6 -26.205 28.050 100



