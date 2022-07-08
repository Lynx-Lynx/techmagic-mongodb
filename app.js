'use strict'

const {mapUser, getRandomFirstName} = require('./util')
const students = require('./students.json');

// db connection and settings
const connection = require('./config/connection')
let userCollection, articleCollection, studentCollection
run()

async function run() {
  await connection.connect()
  await connection.get().dropCollection('users')
  await connection.get().createCollection('users')
  userCollection = connection.get().collection('users')

  await connection.get().dropCollection('articles')
  await connection.get().createCollection('articles')
  articleCollection = connection.get().collection('articles')

  await connection.get().dropCollection('students')
  await connection.get().createCollection('students')
  studentCollection = connection.get().collection('students')

  await example1()
  await example2()
  await example3()
  await example4()
  
  await example5()
  await example6()
  await example7()
  await example8()
  await example9()

  await example10()
  await example11()
  await example12()
  await example13()
  await example14()
  await example15()
  await example16()
  await example17()

  await connection.close()
}

const users = () => {
  const USERS_LIST = [];
  for (let i = 0; i < 6; i++) {
    const user = mapUser();
    i <= 1 ? user.department = 'a'
      : i <= 3 ? user.department = 'b'
      : user.department = 'c';
      USERS_LIST.push(user);
  }
  return USERS_LIST;
};

const articles = () => {
  const ARTICLES_LIST = [];
  for (let i = 0; i < 15; i++) {
    const article = {
      name:  'Mongodb - introduction',
      description: 'Mongodb - text',
      type: '',
      tags: []
  };
    i <= 4 ? article.type = 'a'
      : i <= 9 ? article.type = 'b'
      : article.type = 'c';
      ARTICLES_LIST.push(article);
  }
  return ARTICLES_LIST;
};

// #### Users

// - Create 2 users per department (a, b, c)
async function example1() {

  try {
    await userCollection.insertMany(users());
  } catch (err) {
    console.error(err);
  }
}

// - Delete 1 user from department (a)

async function example2() {
  try {
    await userCollection.deleteOne({ department: 'a' });
  } catch (err) {
    console.error(err);
  }
}

// - Update firstName for users from department (b)

async function example3() {
  try {
    await userCollection.updateMany(
      {department: 'b'},
      {
        $set: {
          firstName: getRandomFirstName()
        }
      }
    );
  } catch (err) {
    console.error(err)
  }
}

// - Find all users from department (c)
async function example4() {
  try {
    await userCollection.find({ department: 'c' })
  } catch (err) {
    console.error(err)
  }
}

// #### Articles

// - Create 5 articles per each type (a, b, c)
async function example5() {
  try {
    await articleCollection.insertMany(articles());
  } catch (err) {
    console.error(err)
  }
}

// - Find articles with type a, and update tag list with next value [‘tag1-a’, ‘tag2-a’, ‘tag3’]
async function example6() {
  try {
    await articleCollection.updateMany(
      {type: 'a'},
      {
        $set: {
          tags: ['tag1-a', 'tag2-a', 'tag3']
        }
      }
    );
  } catch (err) {
    console.error(err)
  }
}

// - Add tags [‘tag2’, ‘tag3’, ‘super’] to other articles except articles from type a
async function example7() {
  try {
    await articleCollection.updateMany(
      {type: {$ne: 'a'}},
      {
        $set: {
          tags: ['tag2', 'tag3', 'super']
        }
      }
    );
  } catch (err) {
    console.error(err)
  }
}

// - - Find all articles that contains tags 'tag2' or 'tag1-a'
async function example8() {
  try {
    await articleCollection.find(
     { tags: { $in: ['tag2', 'tag1-a'] } } 
    );
  } catch (err) {
    console.error(err)
  }
}

// - Pull [tag2, tag1-a] from all articles
async function example9() {
  try {
    await articleCollection.updateMany(
      { },
      { 
        $pull: 
        { tags: { $in: ['tag2', 'tag1-a'] } } 
      }
    );
  } catch (err) {
    console.error(err)
  }
}

// #### Students

// - Import all data from students.json into student collection
async function example10() {

  try {
    await studentCollection.insertMany(students);
  } catch (err) {
    console.error(err);
  }
}

// - Find all students who have the worst score for homework, sort by descent
async function example11() {

  const pipeline = [{
    $unwind: { path: '$scores' }
   }, {
    $match: { 'scores.type': 'homework' }
   }, {
    $sort: { 'scores.score': 1 }
   }, { 
    $limit : 5 
  }, {
    $sort: { 'scores.score': -1 }
  }];

  try {
    await studentCollection.aggregate(pipeline);
  } catch (err) {
    console.error(err);
  }
}

// - Find all students who have the best score for quiz and the worst for homework, sort by ascending
async function example12() {

  const pipeline = [{
    $unwind: { path: '$scores' }
   }, {
    $match: {
     $or: [ { 'scores.type': 'quiz' }, { 'scores.type': 'homework' } ]
    }
   }, {
    $sort: { 'scores.score': -1 }
   }, {
    $bucket: {
     groupBy: '$scores.type',
     boundaries: [ 'homework', 'quiz' ],
     'default': 'quiz',
     output: {
      students: {
       $push: { id: '$_id', name: '$name', scores_type: '$scores.type', score: '$scores.score' }
      }
     }
    }
   }, {
    $unwind: { path: '$students' }
   }, {
    $match: {
     $or: [{
       $and: [
        { 'students.scores_type': 'homework' },
        { 'students.score': { $gte: 99 } }
       ]
      },
      {
       $and: [
        { 'students.scores_type': 'quiz' },
        { 'students.score': { $lte: 1 } }
       ]
      }]
    }
   }, {
    $sort: { 'students.score': 1 }
   }];

  try {
    await studentCollection.aggregate(pipeline);
  } catch (err) {
    console.error(err);
  }
}

// - Find all students who have best score for quiz and exam
async function example13() {

  const pipeline = [{
    $unwind: { path: '$scores' }
   }, {
    $match: {
     $or: [
      { 'scores.type': 'exam' },
      { 'scores.type': 'quiz' }
     ]
    }
   }, {
    $group: {
     _id: '$_id',
     name: { $first: '$name' },
     quizAndExamScore: { $sum: '$scores.score' }
    }
   }, {
    $sort: { quizAndExamScore: -1 }
   }, {
    $limit: 5
   }];

  try {
    await studentCollection.aggregate(pipeline);
  } catch (err) {
    console.error(err);
  }
}

// - Calculate the average score for homework for all students
async function example14() {

  const pipeline = [{
    $unwind: { path: '$scores' }
   }, {
    $match: { 'scores.type': 'homework' }
   }, {
    $group: {
     _id: 'average score',
     avgScore: { $avg: '$scores.score' }
    }
   }];

  try {
    await studentCollection.aggregate(pipeline);
  } catch (err) {
    console.error(err);
  }
}

// - Delete all students that have homework score <= 60
async function example15() {

  const pipeline = [{
    $unwind: { path: '$scores' }
   }, {
    $match: {
     $and: [
      { 'scores.type': 'homework' },
      { 'scores.score': { $lte: 60 } }
     ]
    }
   }];

  try {
    const idList = await studentCollection.aggregate(pipeline).toArray()
      .then(data => data.map(student => student._id));
    await studentCollection.deleteMany({ _id: { $in: idList } });
  } catch (err) {
    console.error(err);
  }
}

// - Mark students that have quiz score => 80
async function example16() {

  const pipeline = [{
    $unwind: { path: '$scores' }
   }, {
    $match: {
     $and: [
      { 'scores.type': 'quiz' },
      { 'scores.score': { $gte: 80 } }
     ]
    }
   }, {
    $set: { mark: 'top student' }
   }];

  try {
    await studentCollection.aggregate(pipeline);
  } catch (err) {
    console.error(err);
  }
}

/* - Write a query that group students by 3 categories (calculate the average grade for three subjects)
  - a => (between 0 and 40)
  - b => (between 40 and 60)
  - c => (between 60 and 100) */
async function example17() {

  const pipeline = [{
    $unwind: { path: '$scores' }
   }, {
    $group: {
     _id: '$_id',
     avgScore: {  $avg: '$scores.score' }
    }
   }, {
    $bucket: {
     groupBy: '$avgScore',
     boundaries: [ 0, 40, 60, 100 ],
     'default': 'Other',
     output: {
     "count": { $sum: 1 },
      students: {
       $push: {
        id: '$_id',
        avg_score: '$avgScore'
       }
      }
     }
    }
   }];

  try {
    await studentCollection.aggregate(pipeline);
  } catch (err) {
    console.error(err);
  }
}