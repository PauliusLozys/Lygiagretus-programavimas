#include <iostream>
#include <omp.h>
#include <nlohmann/json.hpp>
#include <vector>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <thread>
#include <chrono>
#include <tuple>
#include "sha256.h"

using nlohmann::json;
using nlohmann::basic_json;
using std::chrono::high_resolution_clock;

class Student
{
public:
	Student() {}
	Student(basic_json<>& json_val) {
		name = json_val["name"];
		userName = json_val["username"];
		gender = json_val["gender"];
		year = json_val["year"];
		grade = json_val["grade"];
	}

	bool compare(Student& other) const {
		if (year == other.year)
			return grade > other.grade;
		else
			return year > other.year;
	}
	std::string print_all() const {
		std::stringstream ss;
		const std::string seperator = " |";
		ss << std::setw(12) << std::left << name << seperator << std::setw(17) << userName << seperator << std::setw(7) << gender << seperator << std::setw(4) << year << seperator << std::setw(5) << grade;
		return ss.str();
	}
	std::string get_name() const { return name; }
	std::string get_username() const { return userName; }
	std::string get_gender() const { return gender; }
	int get_year() const { return year; }
	double get_grade() const { return grade; }

private:
	std::string name;
	std::string userName;
	std::string gender;
	int year;
	double grade;
};

class Students
{
public:
	std::vector<Student> students;
};

struct Result
{
	Student student;
	std::string hash;

	Result() { }

	Result(Student& student, std::string hash)
		:student(student), hash(hash)
	{
	}
};

class DataMonitor
{
public:
	DataMonitor(const size_t& size)
		: max_size(size)
	{
		count = 0;
		data = new Student[max_size];
		omp_init_lock(&lock);
	}
	~DataMonitor() {
		omp_destroy_lock(&lock);
		delete[] data;
	}

	bool Add(Student& student) {

		bool is_done = false;

#pragma omp critical(monitor_lock)
		{
			if (count < max_size) {
				data[count++] = student;
				is_done = true;
			}
		}
		return is_done;
	}

	std::tuple<Student, bool> Take() {

		Student tmp;
		bool is_done = false;
#pragma omp critical(monitor_lock)
		{
			if (count > 0) {
				tmp = data[--count]; // "Pop" last from the stack(array)
				is_done = true;
			}
		}
		return { tmp, is_done };
	}

	size_t get_size() const { return count; }

private:
	Student* data;
	size_t count;
	size_t max_size;
	omp_lock_t lock;
public:
	bool has_finished = false;
};

class ResultMonitor
{
public:
	ResultMonitor(const size_t& size) 
		:data(new Result[size])
	{
		count = 0;
		omp_init_lock(&lock);
	}
	~ResultMonitor() {
		omp_destroy_lock(&lock);
		delete[] data;
	}

	void Add(Result& result) {

		omp_set_lock(&lock); // lock the critical section
			for (size_t i = 0; i < count; i++) {
				if (data[i].student.compare(result.student)) {

					Result oldRez;
					auto newRez = result;
					for (size_t j = i; j < count + 1; j++) {
						oldRez = data[j];
						data[j] = newRez;
						newRez = oldRez;
					}
					count++;
					omp_unset_lock(&lock); // unlock the critical section
					return;
				}
			}
			data[count++] = result;
			omp_unset_lock(&lock); // unlock the critical section
		}
		

	Result& get(size_t& index) const { return data[index]; }
	size_t get_size() const { return count; }
private:
	Result* data;
	size_t count;
	omp_lock_t lock;
};

void ReadJsonFile(const std::string& file_name, Students& students) {

	std::ifstream fin(file_name);

	auto j = json::parse(fin);

	fin.close();


	for (auto& student : j["students"]) {
		students.students.push_back(Student(student));
	}
	std::cout << "Done parsing Json\n";
}

void WriteTextFile(const std::string& file_name, ResultMonitor& result_monitor) {

	const std::string seperator = " |";
	std::ofstream fout(file_name);
	fout << std::setw(12) << std::left << "Name" << seperator << std::setw(17) << "UserName" << seperator << std::setw(7) << "Gender" << seperator << "Year" << seperator << "Grade" << seperator << "Hash\n";
	for (size_t i = 0; i < result_monitor.get_size(); i++)
	{
		auto rez = result_monitor.get(i);
		fout << rez.student.print_all() << seperator << rez.hash << std::endl;
	}
	fout.close();
	std::cout << "Finished writting the result file\n";
}

void VeryHeavyFunction(DataMonitor& data_monitor, ResultMonitor& result_value) {

	SHA256 hasher;

	while (true) {
		std::tuple<Student, bool> stu;
		bool has_value = false;
		while (!has_value) {
			if (data_monitor.get_size() == 0 && data_monitor.has_finished) {
				return;
			}
			stu = data_monitor.Take();
			has_value = std::get<1>(stu);
		}
		auto s = std::get<0>(stu);
		std::stringstream ss;
		ss << s.get_name() << s.get_year() << s.get_grade() << s.get_username() << s.get_gender();

		auto string_to_hash = hasher(ss.str());

		for (size_t i = 0; i < 100; i++)
		{
			ss.clear();
			ss << string_to_hash << i;
			string_to_hash = hasher(ss.str());
		}

		auto result = Result(std::get<0>(stu), string_to_hash);
		
		// Filtering the result 
		if (result.student.get_grade() > 7) {
			result_value.Add(result);
		}
	}
}

int main() {

	auto t1 = high_resolution_clock::now();

	Students students;
	ReadJsonFile("Data 1.json", students);

	DataMonitor monitor(students.students.size() / 2);
	ResultMonitor result(students.students.size());

#pragma omp parallel // num_threads(2)
	{
		int thread_id = omp_get_thread_num();
		if (thread_id == 0) {
			for (auto& stu : students.students) {
				while (!monitor.Add(stu)); // Trying till its added
			}
			monitor.has_finished = true;
		}
		else {
			VeryHeavyFunction(monitor, result);
		}
	}

	// Print ResultMonitor

	WriteTextFile("rez 1.txt", result);

	auto t2 = high_resolution_clock::now();
	std::chrono::duration<float> duration = t2 - t1;
	std::cout << "Time taken: " << duration.count() << 's' << std::endl;

	std::cout << "\nMain thread killed itself\n";
}