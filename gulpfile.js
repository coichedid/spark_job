var gulp = require('gulp');
var gutil = require('gulp-util');
var shell = require('gulp-shell');
var rename = require('gulp-rename');
const s3 = require('gulp-s3');
const git = require('gulp-git');
const argv = require('yargs').argv;
var fs = require('fs');
var runSequence = require('run-sequence');
const options = {
  cwd:'./docs',
  verbose:true,
}

const options_md = {
  cwd:'./docs',
  verbose:true,
}

const options_venv = {
  cwd:'./',
  verbose:true,
}

var AWS = JSON.parse(fs.readFileSync('sensible_data/aws_config.json'));

gulp.task('increment_version', function(done) {
  var config = JSON.parse(fs.readFileSync('./package_setup.json'));
  var versionStr = config['version'].split('.')
  var version = versionStr.map((item) => parseInt(item))
  version[2] += 1
  config['version'] = version.join('.')
  const json = JSON.stringify(config)
  fs.writeFile('./package_setup.json', json, 'utf8', done)
})

gulp.task('publish', function() {
  var options = { uploadPath: 'sources/spark_jobs/'}
  gulp.src(['./base_processor/BaseProcessor.py']).pipe(s3(AWS,options))
})

gulp.task('copy_docs', function(done) {
  runSequence('copy_README', 'copy_other_docs', done)
})

gulp.task('copy_README', function() {
  gulp.src(['./docs/build/rst/README.md'])
    .pipe(gulp.dest('./', {overwrite:true}))
})

gulp.task('copy_other_docs', function() {
  gulp.src(['./docs/build/rst/processor.md', './docs/build/rst/processor_test.md', './docs/build/rst/provenance.md'])
    .pipe(gulp.dest('./', {overwrite:true}))
})

gulp.task('build_docs', shell.task('make rst', options));

gulp.task('build_md', shell.task('./rst_to_md.sh', options_md));

gulp.task('refresh_docs', function(done) {
  runSequence('build_docs', 'build_md', 'copy_docs', done);
})

gulp.task('docs', ['refresh_docs'], function() {
  gulp.watch(['./docs/source/*.rst','./base_processor/**/*.py'], ['refresh_docs']);
})

gulp.task('push_version', function() {
  return git.revParse({args:'--abbrev-ref HEAD'}, function (err, branch) {
    console.log('current git branch: ' + branch);
    var commitComment = "Initial commit"
    if (argv.m) commitComment = argv.m
    if (argv.branch) branch = argv.branch;

    return gulp.src('./')
          .pipe(git.add())
          .pipe(git.commit(commitComment))
          .on('end', () => {
            git.push('origin', branch, (err) => {
              if (err) {
                console.log(err);
                throw err;
              }
            })
          });
  });
});

gulp.task('deploy_version', function(done){
  runSequence('increment_version','refresh_docs', 'push_version', 'publish', done);
})

gulp.task('test', shell.task('python3.7 -m pytest --show-capture=all -ra', {cwd:'./tests', ignoreErrors:true}));
// gulp.task('test', shell.task('python3.7 -m pytest --show-capture=all -vv -ra', {cwd:'./tests', ignoreErrors:true}));

gulp.task('watch_tests', ['test'], function() {
  gulp.watch(['./tests/*.py','./tests/configs/*.json', './base_processor/**/*.py'], ['test']);
})

gulp.task('run_methodtests', shell.task('python3.7 tests/methodtests.py', {cwd:'./', ignoreErrors:true}));

gulp.task('watch_methodtests', ['run_methodtests'], function(){
  gulp.watch(['./tests/methodtests.py'], ['run_methodtests']);
})

// The key to deploying as a single command is to manage the sequence of events.
gulp.task('default', function(callback) {
  // console.log('Everything is fine!');
  // console.log(gulp);
  // console.log(zeppelinNotebook);
  callback();
});

gulp.task('venv', shell.task('python3.7 -m venv ./venv', options_venv));
