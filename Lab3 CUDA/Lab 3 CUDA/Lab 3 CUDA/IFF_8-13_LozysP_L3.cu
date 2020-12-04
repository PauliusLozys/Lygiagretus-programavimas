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
		sprintf(c_year, "%d", year);
		sprintf(c_grade, "%f", grade);
	}
	
	char name[20];
	int year;
	char c_year[10];
	float grade;
	char c_grade[10];
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
	fin.close();
}

__global__ void process_data(Student *device_students,char *device_results, int* result_space, int *write_index);

int main() {

	Student students[1000];
	read_data("data1.csv", students);	

	int result_space = 30;
	int write_index = 0;

	// Result string
	char *host_results = new char[sizeof(char) * result_space * 1000];

	// Allocate GPU memory
	Student *device_students;
	char *device_results;
	int *device_result_space;
	int *device_write_index;

	cudaMalloc((void**) &device_results		, sizeof(char) * result_space * 1000);
	cudaMalloc((void**) &device_students	, sizeof(Student) * 1000);
	cudaMalloc((void**) &device_result_space, sizeof(int));
	cudaMalloc((void**) &device_write_index , sizeof(int));

	// Copy from CPU to GPU data that is needed
	cudaMemcpy(device_students, 	&students[0],   sizeof(Student) * 1000			  , cudaMemcpyHostToDevice);
	cudaMemcpy(device_result_space, &result_space,  sizeof(int)						  , cudaMemcpyHostToDevice);
	cudaMemcpy(device_write_index , &write_index,   sizeof(int)						  , cudaMemcpyHostToDevice);

	// Run
	process_data<<<1, 243>>>(device_students, device_results, device_result_space, device_write_index);
	cudaDeviceSynchronize();


	auto err = cudaMemcpy(host_results, device_results, sizeof(char) * result_space * 1000, cudaMemcpyDeviceToHost); // copy students to GPU
	std::cout << "Copy to host "<< err << std::endl;
	// std::cout << "Result: \n"<< host_results << std::endl;

	std::cout << "Writting results\n";
	std::ofstream fout("rez.txt");

	fout << host_results; 

	fout.close();
	std::cout << "Finished writting results\n";

	// Fee CPU and GPU memory
	free(host_results);
	cudaFree(device_results);
	cudaFree(device_students);
	cudaFree(device_result_space);
}

__global__ void process_data(Student *device_students,char *device_results, int *result_space, int *write_index) {

	// Calculate the working index range
	const auto work_block = 1000 / blockDim.x;
	int start_index = work_block * threadIdx.x;
	int end_index;

	if (threadIdx.x == blockDim.x - 1) {
		end_index = 1000;
	}
	else {
		end_index = work_block * (threadIdx.x + 1);
	}

	// printf("Thread count: %d\nThread nr: %d\nThread start_index: %d\nThread end_index: %d\nThread work_block: %d\n\n",blockDim.x, threadIdx.x, start_index, end_index, work_block);

	for (auto i = start_index; i < end_index; i++)
	{
		auto student = device_students[i];
		long hash;
		long mul = 1;

		for (size_t d = 0; d < 1000000; d++)
		{
			for (int h = 0; h != 20 ; h++)
			{
				mul = (h % 4 == 0) ? 1 : mul * 256 * i;
				hash += student.name[h] * mul;
			}
		}

		if( hash < 0){
			hash = hash * -1;
		}
		// printf("%d %d %s\n",threadIdx.x, i, student.name);
		char buffer[100];
		
		int current_index = 0;
		for (size_t f = 0; student.name[f] != '\0'; f++)
		{
			buffer[current_index++] = student.name[f];
		}
		buffer[current_index++] = '-';
		int year_index = current_index;
		for (size_t f = 0; student.c_year[f] != '\0'; f++)
		{
			buffer[current_index++] = student.c_year[f];
		}
		buffer[current_index++] = '-';
		for (size_t f = 0; f < 3; f++)
		{
			buffer[current_index++] = student.c_grade[f];
		}
		buffer[current_index++] = '-';
		
		buffer[current_index++] = '|';
		int break_counter = 7;
		for (size_t i = 0; i < hash && break_counter > 0; i+= 255)
		{
			int s = hash / (i + 1); 
			buffer[current_index++] = (char)((s % 125)+33);
			break_counter--;
		}
		buffer[current_index++] = '|';

		// printf("Thread id: %d - %d\n",threadIdx.x,  hash);

		if ((buffer[year_index] - 48) > 2) { // Filter 3, 4
			// printf("%d %s\n", threadIdx.x, buffer);
			int offset = atomicAdd(write_index, 1) * (*result_space);
			bool buffer_ended = false;
			for (size_t j = 0; j < *result_space; j++)
			{
				if(buffer[j] == '\0')
					buffer_ended = true;
				device_results[j + offset] = buffer_ended ? ' ' : buffer[j];
			}
		}




		
	}
	

	
}
