#include "cuda_runtime.h"
#include "device_launch_parameters.h"
#include <fstream>
#include <iostream>

class Student
{
public:
	Student() {}
	Student(std::string _name, int year, float grade, char gender)
		: year(year), grade(grade), gender(gender)
	{
		strcpy(name, _name.c_str());
	}
	
	char name[20];
	int year;
	float grade;
	char gender;
};

void read_data (const char* filePath, Student students[]) {

	std::ifstream fin(filePath);

	size_t index = 0;
	while (!fin.eof())
	{
		std::string name;
		int year;
		float grade;
		char gender;
		fin >> name >> year >> grade >> gender >> std::ws;

		students[index++] = Student(name, year, grade, gender);
	}
}

__global__ void process_data(Student *device_students,char *device_results, int* result_space);

int main() {

	Student students[1000];
	read_data("data1.csv", students);	

	int result_space = 50;

	// Result string
	char *host_results = new char[sizeof(char) * 100 * 1000];

	// Allocate GPU memory
	Student *device_students;
	char *device_results;
	int *device_result_space;

	cudaMalloc((void**) &device_results		, sizeof(char) * result_space * 1000);
	cudaMalloc((void**) &device_students	, sizeof(Student) * 1000);
	cudaMalloc((void**) &device_result_space, sizeof(int));

	// Copy from CPU to GPU data that is needed
	cudaMemcpy(device_students, 	&students[0],   sizeof(Student) * 1000			  , cudaMemcpyHostToDevice);
	cudaMemcpy(device_result_space, &result_space,  sizeof(int)						  , cudaMemcpyHostToDevice);

	// Run
	process_data<<<1, 70>>>(device_students, device_results, device_result_space);
	cudaDeviceSynchronize();


	auto err = cudaMemcpy(host_results, device_results, sizeof(char) * result_space * 1000, cudaMemcpyDeviceToHost); // copy students to GPU
	std::cout << "Copy to host "<< err << std::endl;
	std::cout << "Result: \n"<< host_results << std::endl;


	// Fee CPU and GPU memory
	free(host_results);
	cudaFree(device_results);
	cudaFree(device_students);
	cudaFree(device_result_space);
}

__global__ void process_data(Student *device_students,char *device_results, int* result_space) {

	auto name = device_students[threadIdx.x].name;
	int offset = threadIdx.x * (*result_space);

	bool name_ended = false;
	for (size_t i = 0; i < *result_space; i++)
	{
		if (name[i] == '\0')
			name_ended = true;
		device_results[i + offset] = name_ended ? ' ' : name[i];
	}
}