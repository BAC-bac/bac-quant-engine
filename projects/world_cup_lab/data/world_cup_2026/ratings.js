"use strict;"

var brokenfirefox = window.mozInnerScreenX != null && /Firefox\/[5-9]/.test(window.navigator.userAgent);

window.addEventListener('error', handleErrorEvent, false);
window.onerror = handleError;

var dateGroup = {
  id: 'date',
  columns: [
    { datafield: 'date', condensedWidth: 50, narrowWidth: 60, normalWidth: 130, cssClass: 'match-cell', headerCssClass: 'fixture-header', header1lCssClass: 'match-header' }
  ],
  priority: 3
};

var matchColumn = { datafield: 'match', widthMultiple: 3, condensedWidth: 85, narrowWidth: 115, normalWidth: 260, cssClass: 'match-cell', headerCssClass: 'fixture-header', header1lCssClass: 'match-header' };

var matchGroup = {
  id: 'match',
  columns: [ matchColumn,
    { datafield: 'score', condensedWidth: 15, narrowWidth: 25, normalWidth: 42, cssClass: 'score-cell', headerCssClass: 'match-header' }
  ],
  priority: 0
};

var fixtureGroup = {
  id: 'fixture',
  columns: [ matchColumn ],
  priority: 0
};

var tournamentGroup = {
  id: 'tournament',
  columns: [
    { datafield: 'tournament', widthMultiple: 3, condensedWidth: 100, narrowWidth: 120, normalWidth: 310, cssClass: 'match-cell', headerCssClass: 'fixture-header', header1lCssClass: 'match-header' }
  ],
  priority: 4
};

var ratingsGroup = {
  id: 'changes',
  columns: [
    { datafield: 'changes', condensedWidth: 25, narrowWidth: 35, normalWidth: 62, cssClass: 'change-cell', headerCssClass: 'match-header' },
    { datafield: 'ratings', condensedWidth: 40, narrowWidth: 50, normalWidth: 80, cssClass: 'score-cell', headerCssClass: 'match-header' }
  ],
  priority: 1
};

var ranksGroup = {
  id: 'moves',
  columns: [
    { datafield: 'moves', condensedWidth: 25, narrowWidth: 35, normalWidth: 55, cssClass: 'change-cell', headerCssClass: 'match-header' },
    { datafield: 'ranks', condensedWidth: 32, narrowWidth: 40, normalWidth: 70, cssClass: 'score-cell', headerCssClass: 'match-header' }
  ],
  priority: 2
};

var currentGroup = {
  id: 'current',
  columns: [
    { datafield: 'rank', condensedWidth: 25, narrowWidth: 35, normalWidth: 55, cssClass: 'change-cell', headerCssClass: 'change-header' },
    { datafield: 'rating', condensedWidth: 40, narrowWidth: 50, normalWidth: 75, cssClass: 'score-cell', headerCssClass: 'rating-cell' }
  ],
  priority: 1
};

var winexpGroup = {
  id: 'winexp',
  columns: [
    { datafield: 'winexp', condensedWidth: 32, narrowWidth: 55, normalWidth: 100, cssClass: 'score-cell', headerCssClass: 'winexp-header', header1lCssClass: 'match-header' }
  ],
  priority: 2
};

var estimateGroup = {
  id: 'estimate',
  columns: [
    { datafield: 'draw', condensedWidth: 25, narrowWidth: 35, normalWidth: 60, cssClass: 'score-cell', headerCssClass: 'draw-header' },
    { datafield: 'change1', condensedWidth: 25, narrowWidth: 35, normalWidth: 70, cssClass: 'score-cell', headerCssClass: 'change-header' },
    { datafield: 'change2', condensedWidth: 25, narrowWidth: 35, normalWidth: 70, cssClass: 'score-cell', headerCssClass: 'change-header' },
    { datafield: 'change3', condensedWidth: 25, narrowWidth: 35, normalWidth: 70, cssClass: 'score-cell', headerCssClass: 'change-header' },
    { datafield: 'change4', condensedWidth: 25, narrowWidth: 35, normalWidth: 70, cssClass: 'score-cell', headerCssClass: 'change-header' },
    { datafield: 'change5', condensedWidth: 25, narrowWidth: 35, normalWidth: 70, cssClass: 'score-cell', headerCssClass: 'change-header' }
  ],
  priority: 5
};

var globalRankGroup = {
  id: 'rank',
  showWithNext: true,
  columns: [
    { datafield: 'rank', condensedWidth: 25, narrowWidth: 40, normalWidth: 70, cssClass: 'rating-cell', headerCssClass: 'rating-header', header1lCssClass: 'rating-header-1l' }
  ],
  priority: 2
};

var rankGroup = {
  id: 'rank',
  showWithNext: true,
  columns: [
    { datafield: 'local', condensedWidth: 30, narrowWidth: 40, normalWidth: 60, cssClass: 'local-cell' },
    { datafield: 'global', condensedWidth: 30, narrowWidth: 45, normalWidth: 70, cssClass: 'global-cell' },
  ],
  priority: 2
};

var teamGroup = {
  id: 'team',
  showWithNext: true,
  columns: [
    { datafield: 'team', widthMultiple: 3, condensedWidth: 85, narrowWidth: 115, normalWidth: 260, cssClass: 'team-cell', headerCssClass: 'team-header', header1lCssClass: 'team-header-1l' }
  ],
  priority: 0
};

var ratingGroup = {
  id: 'rating',
  alwaysShowSeparator: true,
  columns: [
    { datafield: 'rating', condensedWidth: 36, narrowWidth: 45, normalWidth: 80, cssClass: 'rating-cell', headerCssClass: 'rating-header', header1lCssClass: 'rating-header-1l' }
  ],
  priority: 1
};

var changeGroup = {
  id: 'change',
  columns: [
    { datafield: 'rank_chg', condensedWidth: 28, narrowWidth: 35, normalWidth: 60, cssClass: 'rating-cell' },
    { datafield: 'rating_chg', condensedWidth: 36, narrowWidth: 40, normalWidth: 80, cssClass: 'rating-cell' }
  ],
  priority: 3
};

var highestGroup = {
  id: 'highest',
  columns: [
    { datafield: 'rank_max', condensedWidth: 28, narrowWidth: 40, normalWidth: 60, cssClass: 'rating-cell' },
    { datafield: 'rating_max', condensedWidth: 36, narrowWidth: 50, normalWidth: 80, cssClass: 'rating-cell' }
  ],
  priority: 100
};

var averageGroup = {
  id: 'average',
  columns: [
    { datafield: 'rank_avg', condensedWidth: 28, narrowWidth: 40, normalWidth: 60, cssClass: 'rating-cell' },
    { datafield: 'rating_avg', condensedWidth: 36, narrowWidth: 50, normalWidth: 80, cssClass: 'rating-cell' }
  ],
  priority: 5
};

var lowestGroup = {
  id: 'lowest',
  columns: [
    { datafield: 'rank_min', condensedWidth: 28, narrowWidth: 40, normalWidth: 60, cssClass: 'rating-cell' },
    { datafield: 'rating_min', condensedWidth: 36, narrowWidth: 50, normalWidth: 80, cssClass: 'rating-cell' }
  ],
  priority: 100
};

var change3mGroup = {
  id: 'change3m',
  columns: [
    { datafield: 'rank_three_month_change', condensedWidth: 25, narrowWidth: 35, normalWidth: 60, cssClass: 'rating-cell' },
    { datafield: 'rating_three_month_change', condensedWidth: 30, narrowWidth: 40, normalWidth: 70, cssClass: 'rating-cell' }
  ],
  priority: 100
};

var change6mGroup = {
  id: 'change6m',
  columns: [
    { datafield: 'rank_six_month_change', condensedWidth: 25, narrowWidth: 35, normalWidth: 60, cssClass: 'rating-cell' },
    { datafield: 'rating_six_month_change', condensedWidth: 30, narrowWidth: 40, normalWidth: 70, cssClass: 'rating-cell' }
  ],
  priority: 100
};

var change1yGroup = {
  id: 'change1y',
  columns: [
    { datafield: 'rank_one_year_change', condensedWidth: 25, narrowWidth: 35, normalWidth: 60, cssClass: 'rating-cell' },
    { datafield: 'rating_one_year_change', condensedWidth: 30, narrowWidth: 40, normalWidth: 70, cssClass: 'rating-cell' }
  ],
  priority: 4
};

var change2yGroup = {
  id: 'change2y',
  columns: [
    { datafield: 'rank_two_year_change', condensedWidth: 25, narrowWidth: 35, normalWidth: 60, cssClass: 'rating-cell' },
    { datafield: 'rating_two_year_change', condensedWidth: 30, narrowWidth: 40, normalWidth: 70, cssClass: 'rating-cell' }
  ],
  priority: 100
};

var change5yGroup = {
  id: 'change5y',
  columns: [
    { datafield: 'rank_five_year_change', condensedWidth: 25, narrowWidth: 35, normalWidth: 60, cssClass: 'rating-cell' },
    { datafield: 'rating_five_year_change', condensedWidth: 30, narrowWidth: 40, normalWidth: 70, cssClass: 'rating-cell' }
  ],
  priority: 100
};

var change10yGroup = {
  id: 'change10y',
  columns: [
    { datafield: 'rank_ten_year_change', condensedWidth: 25, narrowWidth: 35, normalWidth: 60, cssClass: 'rating-cell' },
    { datafield: 'rating_ten_year_change', condensedWidth: 30, narrowWidth: 40, normalWidth: 70, cssClass: 'rating-cell' }
  ],
  priority: 100
};

var matchesGroup = {
  id: 'matches',
  columns: [
    { datafield: 'total', condensedWidth: 30, narrowWidth: 45, normalWidth: 70, cssClass: 'rating-cell' },
    { datafield: 'home', condensedWidth: 30, narrowWidth: 45, normalWidth: 70, cssClass: 'rating-cell' },
    { datafield: 'away', condensedWidth: 30, narrowWidth: 45, normalWidth: 70, cssClass: 'rating-cell' },
    { datafield: 'neutral', condensedWidth: 30, narrowWidth: 45, normalWidth: 70, cssClass: 'rating-cell' },
    { datafield: 'wins', condensedWidth: 30, narrowWidth: 45, normalWidth: 70, cssClass: 'rating-cell' },
    { datafield: 'losses', condensedWidth: 30, narrowWidth: 45, normalWidth: 70, cssClass: 'rating-cell' },
    { datafield: 'draws', condensedWidth: 30, narrowWidth: 45, normalWidth: 70, cssClass: 'rating-cell' }
  ],
  priority: 6
};

var goalsGroup = {
  id: 'goals',
  columns: [
    { datafield: 'for', condensedWidth: 36, narrowWidth: 50, normalWidth: 80, cssClass: 'rating-cell' },
    { datafield: 'against', condensedWidth: 36, narrowWidth: 50, normalWidth: 80, cssClass: 'rating-cell' }
  ],
  priority: 7
};

var ratingGroups = [ globalRankGroup, rankGroup, teamGroup, ratingGroup, changeGroup, highestGroup, averageGroup, lowestGroup, change3mGroup, change6mGroup, change1yGroup, change2yGroup, change5yGroup, change10yGroup, matchesGroup, goalsGroup ];
var ratingHideGroups = [ dateGroup, fixtureGroup, matchGroup, tournamentGroup, ratingsGroup, ranksGroup, changeGroup, currentGroup, winexpGroup, estimateGroup ];
var regionGroups = [ rankGroup, teamGroup, ratingGroup, highestGroup, averageGroup, lowestGroup, change3mGroup, change6mGroup, change1yGroup, change2yGroup, change5yGroup, change10yGroup, matchesGroup, goalsGroup ];
var currentYearGroups = [ globalRankGroup, teamGroup, ratingGroup, changeGroup, highestGroup, averageGroup, lowestGroup, change3mGroup, change6mGroup, change1yGroup, change2yGroup, change5yGroup, change10yGroup, matchesGroup, goalsGroup ];
var yearGroups = [ globalRankGroup, teamGroup, ratingGroup, highestGroup, averageGroup, lowestGroup, change3mGroup, change6mGroup, change1yGroup, change2yGroup, change5yGroup, change10yGroup, matchesGroup, goalsGroup ];
var finalGroups = [ rankGroup, teamGroup, ratingGroup, changeGroup, highestGroup, averageGroup, lowestGroup, change3mGroup, change6mGroup, change1yGroup, change2yGroup, change5yGroup, change10yGroup, matchesGroup, goalsGroup ];
var finalHideGroups = [ dateGroup, fixtureGroup, matchGroup, tournamentGroup, ratingsGroup, ranksGroup, currentGroup, winexpGroup, estimateGroup ];
var fixtureGroups = [ dateGroup, fixtureGroup, tournamentGroup, currentGroup, winexpGroup, estimateGroup ];
var fixtureHideGroups = [ rankGroup, teamGroup, ratingGroup, changeGroup, highestGroup, averageGroup, lowestGroup, change3mGroup, change6mGroup, change1yGroup, change2yGroup, change5yGroup, change10yGroup, matchesGroup, goalsGroup, matchGroup, ratingsGroup, ranksGroup ];
var matchGroups = [ dateGroup, matchGroup, tournamentGroup, ratingsGroup, ranksGroup ];
var matchHideGroups = [ rankGroup, teamGroup, ratingGroup, changeGroup, highestGroup, averageGroup, lowestGroup, change3mGroup, change6mGroup, change1yGroup, change2yGroup, change5yGroup, change10yGroup, matchesGroup, goalsGroup, fixtureGroup, currentGroup, winexpGroup, estimateGroup ];

var ratingData =
{
  includedPriority: 99,
  dataCache: { },
  requests: { },
  menuAllGroups: [ rankGroup, ratingGroup, changeGroup, highestGroup, averageGroup, lowestGroup, change3mGroup, change6mGroup, change1yGroup, change2yGroup, change5yGroup, change10yGroup, dateGroup, tournamentGroup, matchesGroup, goalsGroup, ratingsGroup, ranksGroup, currentGroup, winexpGroup, estimateGroup ],
  menuRatingGroups: [ rankGroup, ratingGroup, highestGroup, averageGroup, lowestGroup, change3mGroup, change6mGroup, change1yGroup, change2yGroup, change5yGroup, change10yGroup, matchesGroup, goalsGroup ],
  menuRatingHideGroups: [ dateGroup, tournamentGroup, ratingsGroup, ranksGroup, changeGroup, currentGroup, winexpGroup, estimateGroup ],
  menuFinalGroups: [ rankGroup, ratingGroup, changeGroup, highestGroup, averageGroup, lowestGroup, change3mGroup, change6mGroup, change1yGroup, change2yGroup, change5yGroup, change10yGroup, matchesGroup, goalsGroup ],
  menuFinalHideGroups: [ dateGroup, tournamentGroup, ratingsGroup, ranksGroup, currentGroup, winexpGroup, estimateGroup ],
  menuMatchGroups: [ dateGroup, tournamentGroup, ratingsGroup, ranksGroup ],
  menuMatchHideGroups: [ rankGroup, ratingGroup, changeGroup, highestGroup, averageGroup, lowestGroup, change3mGroup, change6mGroup, change1yGroup, change2yGroup, change5yGroup, change10yGroup, matchesGroup, goalsGroup, currentGroup, winexpGroup, estimateGroup ],
  menuFixtureGroups: [ dateGroup, tournamentGroup, currentGroup, winexpGroup, estimateGroup ],
  menuFixtureHideGroups: [ rankGroup, ratingGroup, changeGroup, highestGroup, averageGroup, lowestGroup, change3mGroup, change6mGroup, change1yGroup, change2yGroup, change5yGroup, change10yGroup, matchesGroup, goalsGroup, ratingsGroup, ranksGroup ],
  ratingPageData:
  {
    pushRow: pushRatingRow,
    rowHeight: 32,
    condHeight: 36,
    groups: regionGroups,
    hideGroups: ratingHideGroups
  },
  globalPageData:
  {
    pushRow: pushRatingRow,
    rowHeight: 32,
    condHeight: 36,
    groups: [ globalRankGroup, teamGroup, ratingGroup, highestGroup, averageGroup, lowestGroup, change3mGroup, change6mGroup, change1yGroup, change2yGroup, change5yGroup, change10yGroup, matchesGroup, goalsGroup ],
    hideGroups: ratingHideGroups
  },
  currentYearRatingPageData:
  {
    pushRow: pushRatingRow,
    rowHeight: 32,
    condHeight: 36,
    groups: currentYearGroups,
    hideGroups: finalHideGroups
  },
  yearRatingPageData:
  {
    pushRow: pushRatingRow,
    rowHeight: 32,
    condHeight: 36,
    groups: yearGroups,
    hideGroups: ratingHideGroups
  },
  finalRatingPageData:
  {
    pushRow: pushRatingRow,
    rowHeight: 32,
    condHeight: 36,
    groups: finalGroups,
    hideGroups: finalHideGroups
  },
  matchPageData:
  {
    pushRow: pushMatchRow,
    rowHeight: 54,
    condHeight: 60,
    groups: matchGroups,
    hideGroups: matchHideGroups
  },
  fixturePageData:
  {
    pushRow: pushFixtureRow,
    rowHeight: 54,
    condHeight: 60,
    groups: fixtureGroups,
    hideGroups: fixtureHideGroups
  },
  requiredData:
  [
    {
      file: 'teams.tsv',
      func: buildTeams
    },
    {
      file: 'tournaments.tsv',
      func: buildTournaments
    },
    {
      file: 'menu.tsv',
      func: buildMenu
    },
    {
      file: 'en.labels.tsv',
      func: buildLabelDictionary
    },
    {
      file: 'en.teams.tsv',
      func: buildTeamDictionary
    },
    {
      file: 'en.tournaments.tsv',
      func: buildTournamentDictionary
    }
  ]
};

$(document).ready(handleReady);
$(window).on('load', handleLoad);
$(window).on('popstate', handlePopstate);
$(window).resize(handleResize);
window.addEventListener('orientationchange', handleResize, false);

loadRequiredData();

function loadRequiredData()
{
  $.ajaxSetup({ mimeType: "text/plain", timeout: 10000 });
  ratingData.requiredData.forEach(function(data)
  {
    var request = $.ajax(data.file, { cache: false, complete: handleRequestComplete, success: handleRequestSuccess, timeout: 30000 });
    request.requiredData = data;
  });
/*var data = { page: currentPage() };
  if (document.referrer)
    data.referer = document.referrer;
  $.ajax('scripts/visitor.pl', { cache: false, data: data });*/
  $.when(
    $.getJSON("scripts/en.ca-gregorian.json"),
    $.getJSON("scripts/en.numbers.json"),
    $.getJSON("scripts/likelySubtags.json"),
    $.getJSON("scripts/timeData.json"),
    $.getJSON("scripts/weekData.json")
  ).then(function() {
    return [].slice.apply(arguments,[0]).map(function(result) {
      return result[0];
    });
  }).then(Globalize.load).then(function() {
    var globalize = Globalize("en");
    ratingData.dateFormatter = { };
    ratingData.dateFormatter.full = globalize.dateFormatter({ raw: 'EEEE MMMM d y' }); // date: 'full'
    ratingData.dateFormatter.shorter = globalize.dateFormatter({ raw: 'EEE MMM d y' });
    ratingData.dateFormatter.week = globalize.dateFormatter({ raw: 'EEEE<br/>MMMM d' }); // skeleton: 'MMMMEEEEd'
    ratingData.dateFormatter.shortWeek = globalize.dateFormatter({ raw: 'EEE<br/>MMM d' }); // skeleton: 'MMMEEEd'
    ratingData.dateFormatter.long = globalize.dateFormatter({ raw: 'MMMM d<br/>y' }); // date: 'long'
    ratingData.dateFormatter.short = globalize.dateFormatter({ raw: 'MMM d<br/>y' });
    ratingData.dateFormatter.month = globalize.dateFormatter({ raw: 'MMMM<br/>y' });
    ratingData.dateFormatter.fullMonth = globalize.dateFormatter({ raw: 'MMMM y' }); // skeleton: 'yMMMM'
    ratingData.dateFormatter.shortMonth = globalize.dateFormatter({ raw: 'MMM<br/>y' });
    ratingData.dateFormatter.shorterMonth = globalize.dateFormatter({ raw: 'MMM y' });
    ratingData.dateFormatter.year = globalize.dateFormatter({ raw: 'y' }); // skeleton: 'y'
    console.log('globalize loaded ' + parseFloat(window.getComputedStyle(document.body).width) + ' ' + $(document).width() + 'x' + $(document).height() + ' ' + $('body').width() + 'x' + $('body').height() + ' ' + $(window).width() + 'x' + $(window).height() + ' ' + window.innerWidth + 'x' + window.innerHeight + ' ' + window.devicePixelRatio + ' ' + ratingData.pageReady + ' ' + ratingData.pageLoaded + ' ' + ratingData.globalizeLoaded);
    ratingData.globalizeLoaded = true;
    initialDisplay();
  });
}

function logMessage(message)
{
  console.log(message);
  $('div.main').append('<p>' + message + '</p>');
}

function handleError(message, source, lineno, colno, error)
{
  console.log(message);
//$('div.main').append('<p>' + message + ' at ' + source + ' line ' + lineno + ' column ' + colno + '<br/>' + error.stack.replace(/\n/g, '<br/>') + '</p>');
/*var data = currentPage() + ': ' + message + ' at ' + source + ' line ' + lineno + ' column ' + colno;
  if (error)
  {
    data += '\n' + error.name + ': ' + error.message + ' at ' + error.fileName + ' line ' + error.lineNumber + ' column ' + error.columnNumber;
    if (error.stack)
      data += '\n' + error.stack;
  }
  $.ajax('scripts/error.pl', { data: data, method: 'POST' });*/
}

function handleErrorEvent(e)
{
  console.log(e.message);
//$('div.main').append('<p>' + e.message + ' at ' + e.filename + ' line ' + e.lineno + ' column ' + e.colno + '</p>');
/*var data = currentPage() + ': ' + e.message + ' at ' + e.filename + ' line ' + e.lineno + ' column ' + e.colno;
  if (e.error)
  {
    data += '\n' + e.error.name + ': ' + e.error.message + ' at ' + e.error.fileName + ' line ' + e.error.lineNumber + ' column ' + e.error.columnNumber;
    if (e.error.stack)
      data += '\n' + e.error.stack;
  }
  $.ajax('scripts/error.pl', { data: data, method: 'POST' });*/
}

function isHockey()
{
  return /^\/(icehockey|a5)\//.exec(window.location.pathname);
}

function isWorld()
{
  return ratingData.currentPage == 'World';
}

function isGlobal()
{
  return isWorld() || isYearlyRatings();
}

function isTopLevel()
{
  return isWorld() || ratingData.currentPage == 'latest' || ratingData.currentPage == 'fixtures' || ratingData.currentPage == 'graph';
}

function basePage()
{
  var page = currentPage();
  return page == 'graph' || page == 'fixtures' || page == 'latest' ? 'World' : page.replace(/_(latest|fixtures|graph|results|start)/, '');
}

function currentRatings()
{
  return isTopLevel() ? './' : basePage();
}

function currentResults()
{
  return isTopLevel() ? 'latest' : basePage() + '_latest';
}

function currentFixtures()
{
  return isTopLevel() ? 'fixtures' : basePage() + '_fixtures';
}

function currentGraph()
{
  return isTopLevel() ? 'graph' : basePage() + '_graph';
}

function pastRatings()
{
  return basePage();
}

function startRatings()
{
  return basePage() + '_start';
}

function pastResults()
{
  return basePage() + '_results';
}

function isUpsets()
{
  return ratingData.currentPage == 'upsets';
}

function isInfo()
{
  return ratingData.currentPage == 'about' || ratingData.currentPage == 'contact' || ratingData.currentPage == 'recommended';
}

function isLatest()
{
  return ratingData.currentPage == 'latest' || ratingData.currentPage.indexOf('_latest') > 0;
}

function isResults()
{
  return ratingData.currentPage.indexOf('_results') > 0;
}

function isFixtures()
{
  return ratingData.currentPage == 'fixtures' || ratingData.currentPage.indexOf('_fixtures') > 0;
}

function isGraph()
{
  return ratingData.currentPage == 'graph' || ratingData.currentPage.indexOf('_graph') > 0;
}

function isConfederation()
{
  return /^(UEFA|CONMEBOL|CONCACAF|UNCAF|CFU|CAF|UNAF|WAFU|UNIFFAC|CECAFA|COSAFA|AFC|WAFF|CAFA|SAFF|EAFF|AFF|UAFA|OFC|CONIFA|Unaffiliated)$/.exec(ratingData.currentPage) && !isLatest() && !isResults() && !isFixtures() && !isGraph();
}

function isRegion()
{
  return /(^Europe|_Europe|^America|_America|^Africa|^Central_Africa|ern_Africa|^Asia|_Asia|^Caribbean|^Oceania|^Melanesia|^Micronesia|^Polynesia)$/.exec(ratingData.currentPage) && !isLatest() && !isResults() && !isFixtures() && !isGraph();
}

function isYearEndRatings()
{
  return /^\d\d\d\d$/.exec(ratingData.currentPage) && ratingData.endDate[ratingData.currentPage];
}

function isCurrentYearRatings()
{
  return /^\d\d\d\d$/.exec(ratingData.currentPage) && !ratingData.endDate[ratingData.currentPage];
}

/*function isYearFinalRatings()
{
  return isYearEndRatings() && ratingData.startDate[ratingData.currentPage];
}*/

function isYearlyRatings()
{
  return /^\d\d\d\d$/.exec(ratingData.currentPage) || /^\d\d\d\d_start$/.exec(ratingData.currentPage);
}

function isYearly()
{
  return isYearlyRatings() || /^\d\d\d\d_results$|^\d\d\d\d_latest|^\d\d\d\d_graph$/.exec(ratingData.currentPage);
}

function isTournament()
{
  return /_Cup_|_Cup$|_Games|_League|_Championship|Copa_|_Tournament|_Triangulaire|_Trophy/.exec(ratingData.currentPage) && !isLatest() && !isResults() && !isFixtures() && !isGraph();
}

function isRatings()
{
  return isGlobal() || isRegion() || isConfederation() || isTournament();
}

function isPastRatings()
{
  return isRatings() && /^\d\d\d\d/.exec(ratingData.currentPage);
}

function isStartingRatings()
{
  return isRatings() && /^\d\d\d\d/.exec(ratingData.currentPage) && /_start/.exec(ratingData.currentPage);
}

function isFinalRatings()
{
  return isRatings() && /^\d\d\d\d/.exec(ratingData.currentPage) && !/_start/.exec(ratingData.currentPage) && ratingData.endDate[ratingData.currentPage];
}

function isCurRatings()
{
  return isRatings() && /^\d\d\d\d/.exec(ratingData.currentPage) && !/_start/.exec(ratingData.currentPage) && ratingData.startDate[ratingData.currentPage] && !ratingData.endDate[ratingData.currentPage];
}

function isCurrentRatings()
{
  var page = ratingData.currentPage.replace(/_start/, '');
  return isRatings() && (!/^\d\d\d\d/.exec(page) || !/^\d\d\d\d$/.exec(page) && !ratingData.endDate[page]);
}

function isCurrentGraph()
{
  return isGraph() && (!/^\d\d\d\d/.exec(ratingData.currentPage) || !/^\d\d\d\d_graph$/.exec(ratingData.currentPage) && !ratingData.endDate[basePage()]);
}

function isMatches()
{
  return !isRatings() && !isFixtures();
}

function currentPage()
{
  var a = /([^/]*)$/.exec(window.location.pathname);
  return a && a[1] ? a[1] : 'World';
}

function currentPageData()
{
  return ratingData[isFixtures() ? 'fixturePageData' : isMatches() ? 'matchPageData' : isYearEndRatings() ? 'yearRatingPageData' : isCurrentYearRatings() ? 'currentYearRatingPageData' : isFinalRatings() || isCurRatings() ? 'finalRatingPageData' : isGlobal() ? 'globalPageData' : 'ratingPageData'];
}

function checkResize()
{
  delete ratingData.resizeTimeoutID;
  var vw = viewportWidth();
  var vh = viewportHeight();
  console.log('check resize viewport ' + vw + ' ' + vh + ' current ' + ratingData.currentViewportWidth + ' ' + ratingData.currentViewportHeight + ' layout ' + ratingData.currentLayoutWidth + ' included ' + ratingData.includedPriority + ' last ' + ratingData.lastPriority);
  if (vw != ratingData.currentViewportWidth)
  {
    displayPage();
    ratingData.currentViewportWidth = vw;
    ratingData.currentViewportHeight = vh;
  }
  else if (vh != ratingData.currentViewportHeight)
  {
    fitMenuToViewport();
    ratingData.currentViewportHeight = vh;
  }
}

function handleResize()
{
  console.log('resize ' + parseFloat(window.getComputedStyle(document.body).width) + ' ' + $(document).width() + 'x' + $(document).height() + ' ' + $('body').width() + 'x' + $('body').height() + ' ' + $(window).width() + 'x' + $(window).height() + ' ' + window.innerWidth + 'x' + window.innerHeight + ' ' + window.devicePixelRatio);
  if (ratingData.resizeTimeoutID)
    clearTimeout(ratingData.resizeTimeoutID);
  ratingData.resizeTimeoutID = setTimeout(checkResize, 50);
/*if (ratingData.resizeSeen)
    return;
  ratingData.resizeSeen = true;
  var vw = Math.min(parseFloat(window.getComputedStyle(document.body).width), $(window).width(), window.innerWidth);
  var max = 0, over = 0, total = 0;
  $(':visible').each(function(index, element)
  {
    total++;
    var rect = element.getBoundingClientRect();
    var l = parseFloat(rect.left);
    var r = parseFloat(rect.right);
    if (l > max)
      max = l;
    if (r > max)
      max = r;
    if (l > vw || r > vw)
    {
      over++;
      console.log('over ' + element.tagName + ' class ' + element.className + ' id ' + element.id + ' ' + l + ' ' + r);
      //console.log($('<div/>').text($(this)[0].outerHTML).html());
    }
  });
  console.log('Total ' + total + ' max ' + max + ' over ' + over);*/
}

function handlePopstate(e)
{
console.log('popstate scroll top ' + $(window).scrollTop() + ' broken firefox ' + brokenfirefox);
  if (brokenfirefox)
    ratingData.scrollToTop = true;
  displayPage(true);
}

function textWidth(text, size, condensed)
{
  size = size || (condensed ? '14pt' : '12pt');
  // re-use canvas object for better performance
  var canvas = textWidth.canvas || (textWidth.canvas = document.createElement("canvas"));
  var context = canvas.getContext("2d");
  context.font = size + (condensed ? " Open Sans Condensed Light" : " Open Sans Light");
  var metrics = context.measureText(text);
  //console.log('textWidth ' + text + ' ' + context.font + ' ' + metrics.width);
  return metrics.width;
}

function handleReady()
{
  console.log('ready ' + parseFloat(window.getComputedStyle(document.body).width) + ' ' + $(document).width() + 'x' + $(document).height() + ' ' + $('body').width() + 'x' + $('body').height() + ' ' + $(window).width() + 'x' + $(window).height() + ' ' + window.innerWidth + 'x' + window.innerHeight + ' ' + window.devicePixelRatio + ' ' + ratingData.pageReady + ' ' + ratingData.pageLoaded + ' ' + ratingData.globalizeLoaded);
  ratingData.pageReady = true;
  initialDisplay();
}

function handleLoad()
{
  console.log('load ' + parseFloat(window.getComputedStyle(document.body).width) + ' ' + $(document).width() + 'x' + $(document).height() + ' ' + $('body').width() + 'x' + $('body').height() + ' ' + $(window).width() + 'x' + $(window).height() + ' ' + window.innerWidth + 'x' + window.innerHeight + ' ' + window.devicePixelRatio + ' ' + ratingData.pageReady + ' ' + ratingData.pageLoaded + ' ' + ratingData.globalizeLoaded);
  ratingData.pageLoaded = true;
  initialDisplay();
}

function initialDisplay()
{
  if (!ratingData.pageLoaded || !ratingData.pageReady || !ratingData.globalizeLoaded || ratingData.requiredData.some(function(data) { return !data.ready; }))
    return;
  createMenu();
  displayPage(true);
  setupScrolling();
}

function requestData()
{
  $('body').addClass('wait');
  var maxWidth = 0, maxLabel, condWidth = 0, condLabel;
  for (var labels in ratingData.teamDictionary)
    if (ratingData.teamDictionary.hasOwnProperty(labels))
    {
      var label = ratingData.teamDictionary[labels][ratingData.teamDictionary[labels].length - 1];
      var width = textWidth(label);
      if (width > maxWidth)
      {
        maxWidth = width;
        maxLabel = label;
      }
      width = textWidth(label, undefined, true);
      if (width > condWidth)
      {
        condWidth = width;
        condLabel = label;
      }
    }
  console.log('Max label ' + maxLabel + ' width ' + maxWidth + ' cond ' + condLabel + ' width ' + condWidth);
  startRequest(ratingData.currentPage, 0);
}

function startRequest(page, count)
{
  if (page === undefined || count === undefined || ratingData.requests[page] || ratingData.dataCache.hasOwnProperty(page))
    return;
  console.log('requesting ' + page + '.tsv');
  var request = $.ajax(page + '.tsv', { cache: false, complete: handleComplete, success: handleSuccess });
  request.page = page;
  request.retryCount = count;
  ratingData.requests[page] = request;
}

function handleComplete(xhr, text)
{
  var page = xhr.page;
  var count = xhr.retryCount;
  console.log('complete ' + count + ' ' + text + ' ' + page);
  delete ratingData.requests[page];
  if (text != 'success' && count < 10)
    setTimeout(startRequest, count * 1000, page, count + 1);
  else if (page == ratingData.currentPage)
    $('body').removeClass('wait');
}

function handleSuccess(data, text, xhr)
{
  var page = xhr.page;
  console.log('success ' + text + ' ' + page);
  delete ratingData.requests[page];
  if (text == 'success' && data !== undefined && ratingData.dataCache[page] != data)
  {
    ratingData.dataCache[page] = data;
    if (page == ratingData.currentPage)
    {
      ratingData.ratingAsOfDate = new Date(xhr.getResponseHeader("Last-Modified"));
      displayPage(true);
    }
  }
}

function handleRequestComplete(xhr, text)
{
  console.log('complete ' + text + ' ' + xhr.requiredData.file);
}

function handleRequestSuccess(data, text, xhr)
{
  console.log('success ' + text + ' ' + xhr.requiredData.file);
  xhr.requiredData.func(data);
  xhr.requiredData.ready = true;
  initialDisplay();
}

function buildTeams(data)
{
  ratingData.successor = { };
  data.split('\n').forEach(function(line)
  {
    var values = line.split('\t');
    ratingData.successor[values[0]] = values[1];
  });
}

function buildTournaments(data)
{
  ratingData.tournamentMap = { };
  data.split('\n').forEach(function(line)
  {
    var values = line.split('\t');
    ratingData.tournamentMap[values.shift()] = values;
  });
}

function buildMenu(data)
{
  ratingData.dataCache.menu = data;
}

function buildLabelDictionary(data)
{
  ratingData.labelDictionary = { };
  data.split('\n').forEach(function(line)
  {
    var values = line.split('\t');
    ratingData.labelDictionary[values.shift()] = values;
  });
}

function buildTeamDictionary(data)
{
  ratingData.teamCode = { };
  ratingData.teamDictionary = { };
  ratingData.teamLocativeDictionary = { };
  data.split('\n').forEach(function(line)
  {
    var values = line.split('\t');
    var code = values.shift();
    var matches = /(.*)_loc$/.exec(code);
    if (matches)
      ratingData.teamLocativeDictionary[matches[1]] = values;
    else
    {
      ratingData.teamDictionary[code] = values;
      ratingData.teamCode[pageName(values[0])] = code;
    }
  });
}

function buildTournamentDictionary(data)
{
  ratingData.tournamentCode = { };
  ratingData.tournamentDictionary = { };
  data.split('\n').forEach(function(line)
  {
    var values = line.split('\t');
    var code = values.shift();
    ratingData.tournamentDictionary[code] = values;
    ratingData.tournamentCode[pageName(values[0])] = code;
  });
}

function pageName(text)
{
  return text ? text.replace(/ /g, '_').replace(/[àáâãäå]/g, 'a').replace(/ç/g, 'c').replace(/[èéêë]/g, 'e').replace(/[ìíîï]/g, 'i').replace(/[òóôõö]/g, 'o').replace(/[ùúûü]/g, 'u').replace(/[ñ]/g, 'n') : undefined;
}

function pushRatingRow(line)
{
  var fields = line.split('\t');
  var row = { };
  row.local = fields[0];
  row[isGlobal() ? 'rank' : 'global'] = fields[1];
  var team = fields[2];
  var page = pageName(ratingData.teamDictionary[ratingData.successor[team] || team][0]);
  team = ratingData.teamDictionary[team];
  team = fitText(teamGroup.columns[0].layout.width, team);
  row.team = makeLink(page, team);
  row.rating = fields[3];
  row.rank_max = fields[4];
  row.rating_max = fields[5];
  row.rank_avg = fields[6];
  row.rating_avg = fields[7];
  row.rank_min = fields[8];
  row.rating_min = fields[9];
  row.rank_three_month_change = fields[10];
  row.rating_three_month_change = fields[11];
  row.rank_six_month_change = fields[12];
  row.rating_six_month_change = fields[13];
  row.rank_one_year_change = fields[14];
  row.rating_one_year_change = fields[15];
  row.rank_two_year_change = fields[16];
  row.rating_two_year_change = fields[17];
  row.rank_five_year_change = fields[18];
  row.rating_five_year_change = fields[19];
  row.rank_ten_year_change = fields[20];
  row.rating_ten_year_change = fields[21];
  row.total = fields[22];
  row.home = fields[23];
  row.away = fields[24];
  row.neutral = fields[25];
  row.wins = fields[26];
  row.losses = fields[27];
  row.draws = fields[28];
  row.for = fields[29];
  row.against = fields[30];
  row.rank_chg = fields[31];
  row.rating_chg = fields[32];
  ratingData.dataArray.push(row);
}

function formatDate(year, month, day)
{
  month--;
  var normal = ratingData.currentLayout == 'normal';
  var asofyear = ratingData.ratingAsOfDate.getFullYear();
  var asofmonth = ratingData.ratingAsOfDate.getMonth();
  var week = (isLatest() || isFixtures()) && day > 0 && (year == asofyear && month > asofmonth - 6 && month < asofmonth + 6 || year == asofyear - 1 && month > asofmonth + 6 || year == asofyear + 1 && month < asofmonth - 6);
  var date = new Date(year, month, day);
  return week && normal ? ratingData.dateFormatter.week(date)
    : week ? ratingData.dateFormatter.shortWeek(date)
    : day > 0 && normal ? ratingData.dateFormatter.long(date)
    : day > 0 ? ratingData.dateFormatter.short(date)
    : month >= 0 && normal ? ratingData.dateFormatter.month(new Date(year, month))
    : month >= 0 ? ratingData.dateFormatter.shortMonth(new Date(year, month))
    : ratingData.dateFormatter.year(new Date(year, 1));
}

function formatMatch(team1, team2)
{
  var width = matchColumn.layout.width - 5;
  var page1 = pageName(ratingData.teamDictionary[ratingData.successor[team1] || team1][0]);
  team1 = ratingData.teamDictionary[team1];
  team1 = fitText(width, team1);
  var page2 = pageName(ratingData.teamDictionary[ratingData.successor[team2] || team2][0]);
  team2 = ratingData.teamDictionary[team2];
  team2 = fitText(width, team2);
  return (page1 == ratingData.currentPage ? team1 : makeLink(page1, team1)) + '<br/>' + (page2 == ratingData.currentPage ? team2 : makeLink(page2, team2));
}

function formatTournament(tournament, venue, year, month, day)
{
  var layout = tournamentGroup.columns[0].layout;
  if (!layout)
    return '';
  var width = layout.width - (ratingData.currentLayout == 'normal' ? 20 : 10);
  month--;
  var tournpages = new Array();
  var tourns = ratingData.tournamentMap.hasOwnProperty(tournament) ? ratingData.tournamentMap[tournament] : [ tournament ];
  tourns.forEach(function(tourn)
  {
    var tournpage;
    if (ratingData.tournamentPage.hasOwnProperty(tourn))
      ratingData.tournamentPage[tourn].some(function(tournamentPage)
      {
        tournpage = tournamentPage.page;
        if (tournamentPage.startDate)
        {
          var startDate = new Date(tournamentPage.startDate.replace(/-/g, '/'));
          var startYear = startDate.getFullYear();
          var startMonth = startDate.getMonth();
          var startDay = startDate.getDate();
  console.log('start ' + year + ' ' + startYear + ' ' + month + ' ' + startMonth + ' ' + day + ' ' + startDay);
          if (year < startYear || year == startYear && (month < startMonth || month == startMonth && day <= startDay))
            tournpage = '';
        }
        if (tournpage && tournamentPage.endDate)
        {
          var endDate = new Date(tournamentPage.endDate.replace(/-/g, '/'));
          var endYear = endDate.getFullYear();
          var endMonth = endDate.getMonth();
          var endDay = endDate.getDate();
  console.log('end ' + year + ' ' + endYear + ' ' + month + ' ' + endMonth + ' ' + day + ' ' + endDay);
          if (year > endYear || year == endYear && (month > endMonth || month == endMonth && day > endDay))
            tournpage = '';
        }
        return tournpage;
      });
    tournpages.push(tournpage);
  });
  var result = '';
  tournament = fitText(width, ratingData.tournamentDictionary[tournament]);
  var separator = ' and ';
  var parts = tournament.split(separator);
  if (parts.length == 1 && tournpages.length > 1)
  {
    separator = ' & ';
    parts = tournament.split(separator);
  }
  parts.forEach(function(part, index)
  {
    if (index > 0)
      result += separator;
    var tournpage;
    if (index < tournpages.length)
      tournpage = tournpages[index];
    result += tournpage ? makeLink(tournpage, part) : part;
console.log('part ' + part + ' page ' + tournpage + ' result ' + result);
  });
  result += '<br/>';
  if (venue)
  {
    var venuepage = pageName(ratingData.teamDictionary[ratingData.successor[venue] || venue][0]);
    venue = ratingData.teamLocativeDictionary[venue] || ratingData.teamDictionary[venue].map(function(text) { return ratingData.labelDictionary.locativePreposition + ' ' + text; });
    venue = fitText(width, venue);
    result += venuepage == ratingData.currentPage ? venue : makeLink(venuepage, venue);
  }
  return result;
}

function formatChange(change)
{
  return (change > 0 ? '+' : change < 0 ? '&minus;' : '') + Math.abs(change) + '<br/>' + (change < 0 ? '+' : change > 0 ? '&minus;' : '') + Math.abs(change);
}

function pushMatchRow(line)
{
  var fields = line.split('\t');
  var row = { };
  row.date = formatDate(fields[0], fields[1], fields[2]);
  row.match = formatMatch(fields[3], fields[4]);
  row.score = fields[5] + '<br/>' + fields[6];
  row.tournament = formatTournament(fields[7], fields[8] || fields[3], fields[0], fields[1], fields[2]);
  row.changes = formatChange(fields[9]);
  row.ratings = fields[10] + '<br/>' + fields[11];
  row.moves = fields[12] + '<br/>' + fields[13];
  row.ranks = fields[14] + '<br/>' + fields[15];
  ratingData.dataArray.push(row);
}

function pushFixtureRow(line)
{
  var fields = line.split('\t');
  var row = { };
  row.date = formatDate(fields[0], fields[1], fields[2]);
  row.match = formatMatch(fields[3], fields[4]);
  row.tournament = formatTournament(fields[5], fields[6], fields[0], fields[1], fields[2]);
  console.log('Field 8 "' + fields[8] + '" equals "0" ' + fields[8] === '0');
  row.rank = (fields[7] === '0' ? '&minus;' : fields[7]) + '<br/>' + (fields[8] === '0' ? '&minus;' : fields[8]);
  row.rating = fields[9] + '<br/>' + fields[10];
  var we1 = fields[11];
  var we2 = Math.round(1000 - we1 * 10) / 10;
  if ((we2 > 98 || we2 < 2) && /^\d+$/.exec(we2))
    we2 += '.0';
  row.winexp = we1 + '%<br/>' + we2 + '%';
  row.draw = formatChange(fields[12]);
  row.change1 = fields[13] + '<br/>' + fields[14];
  row.change2 = fields[15] + '<br/>' + fields[16];
  row.change3 = fields[17] + '<br/>' + fields[18];
  row.change4 = fields[19] + '<br/>' + fields[20];
  row.change5 = fields[21] + '<br/>' + fields[22];
  ratingData.dataArray.push(row);
}

function viewportWidth()
{
  return Math.floor(Math.min(parseFloat(window.getComputedStyle(document.body).width), $(window).width(), window.innerWidth));
}

function viewportHeight()
{
  return Math.floor(Math.min($(window).height(), window.innerHeight));
}

function currentLayoutWidth()
{
  return Math.min(ratingData.currentLayoutWidth, viewportWidth());
}

function textFits(width, text)
{
  width -= ratingData.currentLayout == 'condensed' ? 5 : ratingData.currentLayout == 'narrow' ? 10 : 15;
  //if (/^Win/.exec(text))
  //  console.log('textFits ' + width + ' ' + text + ' ' + textWidth(text));
  return textWidth(text) <= width;
}

function fitText(width, texts)
{
  // width -= ratingData.currentLayout == 'condensed' ? 5 : ratingData.currentLayout == 'narrow' ? 10 : 15;
  //console.log('fitText ' + width + ' ' + font + ' ' + texts[0]);
  var result;
  texts.some(function(text)
  {
    result = text;
    if (textWidth(text.replace(/&amp;/g, '&'), undefined, ratingData.currentLayout == 'condensed') <= width)
      return text;
  });
  return result;
}

function fitHeaderText(width, fontsize, texts)
{
  //console.log('fitHeaderText ' + width + ' ' + fontsize);
  var result = { };
  texts.some(function(text)
  {
    var w = textWidth(text, fontsize);
    if (w <= width)
    {
      result = { text: text, condensed: false, width: w };
      return result;
    }
  });
  if (result.text)
    return result;
  texts.some(function(text)
  {
    result.text = text;
    result.condensed = true;
    result.width = textWidth(text, fontsize, true);
    if (result.width <= width)
      return result;
  });
  return result;
}

function fitColumnHeaderText(width, texts)
{
  width -= ratingData.currentLayout == 'condensed' ? 5 : ratingData.currentLayout == 'narrow' ? 10 : 15;
  return fitHeaderText(width, '12pt', texts);
}

function fitTwoLineHeaderText(width, texts)
{
  //console.log('fitTwoLineHeaderText ' + width + ' ' + fontsize);
  width -= ratingData.currentLayout == 'condensed' ? 5 : ratingData.currentLayout == 'narrow' ? 10 : 15;
  var result = { };
  var w;
  texts.some(function(text)
  {
    var words = /^(.+) (.+)$/.exec(text);
    if (words)
      words.shift();
    else
      words = [ text ];
    if (words.every(function(t) { w = textWidth(t); return w <= width; }))
    {
      result = { text: text, condensed: false, width: w };
      return result;
    }
  });
  if (result.text)
    return result;
  result.condensed = true;
  texts.some(function(text)
  {
    result.text = text;
    var words = /^(.+) (.+)$/.exec(text);
    if (words)
      words.shift();
    else
      words = [ text ];
    if (words.every(function(t) { w = textWidth(t); return w <= width; }))
      return result;
  });
  result.width = w;
  return result;
}

function toggleGroup(groupid)
{
  console.log('toggle ' + groupid);
  var show, priority, groups = isFixtures() ? fixtureGroups : isMatches() ? matchGroups : ratingGroups;
  groups.forEach(function(group)
  {
    if (group.id == groupid)
    {
      priority = group.priority;
      show = priority > ratingData.includedPriority;
    }
  });
  if (show)
  {
    groups.forEach(function(group)
    {
      if (group.id == groupid)
        group.priority = 1;
      else if (group.priority > 0 && group.priority < 99)
        group.priority ++;
    });
  }
  else
  {
    groups.forEach(function(group)
    {
      if (group.id == groupid)
        group.priority = 100;
      else if (group.priority > priority && group.priority <= 99)
        group.priority --;
    });
    //displayVerticalLines();
  }
  $('#checkmark' + groupid).css('visibility', show ? 'visible' : 'hidden');
  displayPage(true);
}

function handleClick(id)
{
  console.log('click ' + id);
  if (id.indexOf('group') == 0)
    toggleGroup(id.substring(5));
//else if (id.indexOf('dropdown') != 0)
//  handleLink(id);
}

function handleLink(id)
{
  console.log('link ' + id);
  if (id == ratingData.currentPage)
    return;
  if (id == "World")
    id = "./";
  history.pushState({ page: id }, '', id);
  ratingData.scrollToTop = true;
  displayPage(true);
}

function displayPage(force)
{
  if (!ratingData.pageLoaded || !ratingData.pageReady || !ratingData.globalizeLoaded || ratingData.requiredData.some(function(data) { return !data.ready; }))
    return;
  if (force)
  {
    delete ratingData.currentLayoutWidth;
    delete ratingData.dateDate;
    delete ratingData.dateLabel;
    delete ratingData.dateIncrease;
    delete ratingData.legendText;
  }
  var page = currentPage();
  if (page != ratingData.currentPage)
  {
    ratingData.currentPage = page;
    console.log('current page ' + page);
  }
  console.log('display ' + page + ' ' + window.location.pathname);
  if (!ratingData.dataCache.hasOwnProperty(page))
    requestData();
  else if (isInfo())
    displayInfo();
  else if (isGraph())
    displayGraph();
  else
    displayTable();
}

function makeLink(name, label)
{
  return '<a href="' + name + '" onclick="handleLink(\'' + name + '\'); return false;">' + label + '</a>';
}

function makeCheckbox(id, label)
{
  return '<span id="' + id + '">✔</span>&nbsp;' + label.replace(/ /g, '&nbsp;'); // ✓
}

function showVerticalLine(group, position)
{
  var id = 'vl_' + group.id;
  if ($('#' + id).length == 0)
    $('#maindiv').append("<div id='" + id + "' class='verticalline'></div>");
  $('#' + id).css('left', position + 'px');
  $('#' + id).css('display', '');
}

function hideVerticalLine(group)
{
  var id = 'vl_' + group.id;
  if ($('#' + id).length)
    $('#' + id).css('display', 'none');
}

function fitIndex(item, labelIndex)
{
  if (!item.labels)
    return;
  var labelCount = item.labels.length;
  if (!labelIndex[labelCount])
    labelIndex[labelCount] = 0;
  while (labelIndex[labelCount] < labelCount - 1 && !textFits(item.layout.width, item.labels[labelIndex[labelCount]]))
    labelIndex[labelCount] ++;
  //console.log('fitIndex ' + (item.id || item.datafield) + ' count ' + labelCount + ' index ' + labelIndex[labelCount] + ' label ' + item.labels[labelIndex[labelCount]]);
}

function calculateWidth()
{
  var lastVisibleGroup;
  var pageData = currentPageData();
  var lastPriority = 0;
  pageData.groups.forEach(function(group)
  {
    if (group.priority > lastPriority && group.priority <= 99)
      lastPriority = group.priority;
  });
  var vw = viewportWidth();
  var availableWidth = vw;
  var widthMultiples = new Array(lastPriority + 1);
  var condensedWidths = new Array(lastPriority + 1);
  var narrowWidths = new Array(lastPriority + 1);
  var normalWidths = new Array(lastPriority + 1);
  var columnCounts = new Array(lastPriority + 1);
  for (var i = 0; i <= lastPriority; i++)
    widthMultiples[i] = condensedWidths[i] = narrowWidths[i] = normalWidths[i] = columnCounts[i] = 0;
  pageData.groups.forEach(function(group)
  {
    group.columns.forEach(function(column)
    {
      if (group.priority <= 99)
        for (var i = group.priority; i <= lastPriority; i++)
        {
          widthMultiples[i] += column.widthMultiple || 1;
          condensedWidths[i] += column.condensedWidth;
          narrowWidths[i] += column.narrowWidth;
          normalWidths[i] += column.normalWidth;
          columnCounts[i] ++;
        }
    });
  });
  console.log('Layout widths: condensed ' + condensedWidths.join(' ') + ' narrow ' + narrowWidths.join(' ') + ' normal ' + normalWidths.join(' ') + ' multiples ' + widthMultiples.join(' '));
  var includedPriority = 0;
  for (var i = 1; i <= lastPriority; i++)
    if (availableWidth >= narrowWidths[i])
      includedPriority = i;
    else
      break;
  if (widthMultiples[includedPriority] < 6)
    for (var i = lastPriority; i >= 0; i--)
      if (availableWidth >= condensedWidths[i])
      {
        includedPriority = i;
        break;
      }
  var layout;
  var neededWidth;
  if (availableWidth >= normalWidths[includedPriority])
  {
    layout = 'normal';
    neededWidth = normalWidths[includedPriority];
  }
  else if (availableWidth >= narrowWidths[includedPriority])
  {
    layout = 'narrow';
    neededWidth = narrowWidths[includedPriority];
  }
  else
  {
    layout = 'condensed';
    neededWidth = condensedWidths[includedPriority];
  }
  var columnCount = columnCounts[includedPriority];
  if (availableWidth > normalWidths[includedPriority])
    availableWidth = normalWidths[includedPriority];
  if (!ratingData.menuResizing)
    ratingData.showMainMenu = vw - availableWidth >= 180;
  displayMenu();
  var menuWidth = $('#accordion-menu').width();
  if (ratingData.showMainMenu && availableWidth > vw - menuWidth - 32)
    availableWidth = vw - menuWidth - 32;
  console.log('Layout ' + layout + ' width ' + neededWidth + ' columns ' + columnCount + ' viewport ' + vw + ' menu ' + menuWidth + ' available ' + availableWidth + ' included priority ' + includedPriority);
  var widthUnchanged = ratingData.currentLayoutWidth == availableWidth;
  ratingData.currentLayoutWidth = availableWidth;
  finishStyles();
  if (widthUnchanged)
    return false;
  $('#maindiv').removeClass('morepadding');
  var layoutCssClass = layout == 'narrow' ? ' narrow-layout' : layout == 'condensed' ? ' condensed-layout' : '';
  var groupLabelIndex = [ ];
  var columnLabelIndex = [ ];
  ratingData.currentViewportWidth = vw;
  ratingData.currentLayout = layout;
  ratingData.lastPriority = lastPriority;
  ratingData.includedPriority = includedPriority;
  ratingData.columnLayout = new Array();
  pageData.groups.forEach(function(group)
  {
    group.visible = group.priority <= ratingData.includedPriority;
    if (group.visible)
    {
      lastVisibleGroup = group;
      var groupWidth = 0;
      group.columns.forEach(function(column)
      {
        column.labels = ratingData.labelDictionary[column.datafield + 'Header'];
        if (!column.labels)
        {
          var matches = /([^_]+)_/.exec(column.datafield);
          if (matches)
            column.labels = ratingData.labelDictionary[matches[1] + 'Header'];
        }
        column.layout = { id: column.datafield, field: column.datafield, resizable: false };
        var columnWidth = column[ratingData.currentLayout + 'Width'];
        if (availableWidth != neededWidth)
          columnWidth = Math.floor(columnWidth * availableWidth / neededWidth);
        //console.log('Column ' + column.datafield + ' width ' + column[ratingData.currentLayout + 'Width'] + ' columns ' + columnCount + ' available ' + availableWidth + ' needed ' + neededWidth + ' result ' + columnWidth);
        columnCount --;
        availableWidth -= columnWidth;
        neededWidth -= column[ratingData.currentLayout + 'Width'];
        column.layout.width = columnWidth;
        if (column.layout.width < 30)
          column.layout.minWidth = column.layout.width;
        groupWidth += columnWidth;
        if (group.id != 'winexp')
          fitIndex(column, columnLabelIndex);
        ratingData.columnLayout.push(column.layout);
      });
      if (group.columns.length > 1 && group.columns[1].labels)
      {
        group.labels = ratingData.labelDictionary[group.id + 'Header'];
        group.layout = { width: groupWidth };
      }
      if (group.layout)
        fitIndex(group, groupLabelIndex);
    }
  });
  ratingData.showGroupHeaders = !isMatches() && (isFixtures() ? currentGroup.priority <= ratingData.includedPriority || estimateGroup.priority <= ratingData.includedPriority
    : lastVisibleGroup.id != 'team' && lastVisibleGroup.id != 'rating' || !isGlobal() && rankGroup.priority <= ratingData.includedPriority);
  lastVisibleGroup = undefined;
  pageData.groups.forEach(function(group)
  {
    if (group.visible)
    {
      group.columns.forEach(function(column)
      {
        if (column.labels)
          column.layout.name = group.id != 'winexp' ? column.labels[columnLabelIndex[column.labels.length]]
            : ratingData.showGroupHeaders ? fitTwoLineHeaderText(column.layout.width, column.labels).text
            : fitColumnHeaderText(column.layout.width, column.labels).text;
        var cssClass = layoutCssClass;
        if (group.id == 'team' && !lastVisibleGroup)
          cssClass += ' left-padding';
        if (column.cssClass)
          column.layout.headerCssClass = column.layout.cssClass = column.cssClass + ' ' + cssClass;
        if (!ratingData.showGroupHeaders && column.header1lCssClass)
          column.layout.headerCssClass = column.header1lCssClass + ' ' + cssClass;
        else if (column.headerCssClass)
          column.layout.headerCssClass = column.headerCssClass + ' ' + cssClass;
        //console.log('Column ' + column.datafield + ' name ' + column.layout.name + ' count ' + column.labels.length + ' index ' + columnLabelIndex[column.labels.length] + ' width ' + column.layout.width);
      });
      if (group.layout)
        group.layout.name = group.labels[groupLabelIndex[group.labels.length]];
      lastVisibleGroup = group;
    }
  });
  updateColumnMenu();
  return true;
}

function displayVerticalLines()
{
  var lastVisibleGroup, linePosition = 0;
  currentPageData().groups.forEach(function(group)
  {
    var showSeparator = group.alwaysShowSeparator;
    if (group.priority <= ratingData.includedPriority)
    {
      group.columns.forEach(function(column)
      {
        linePosition += column.layout.width;
        showSeparator = true;
        //console.log('Group ' + group.id + ' column ' + column.datafield + ' priority ' + group.priority + '/' + ratingData.includedPriority + ' width ' + column.layout.width + '/' + linePosition);
      });
    }
    if (showSeparator && !group.showWithNext)
    {
      showVerticalLine(group, linePosition);
      lastVisibleGroup = group;
    }
    else
      hideVerticalLine(group);
  });
  hideVerticalLine(lastVisibleGroup);
}

function displayHeaders()
{
  var upsetPage = isUpsets();
  var matchPage = isMatches();
  var fixturePage = isFixtures();
  var graphPage = isGraph();
  var infoPage = isInfo();
  var page = basePage();
  var text;
  if (isWorld() || infoPage)
    text = ratingData.labelDictionary.mainTitle;
  else
  {
    var yearly = isYearly();
    var label = ratingData.currentPage == 'latest' ? ratingData.labelDictionary.latestTitle
      : ratingData.currentPage == 'fixtures' ? ratingData.labelDictionary.fixturesTitle
      : ratingData.currentPage == 'graph' ? ratingData.labelDictionary.graphTitle
      : ratingData.currentPage == '1901_results' || ratingData.currentPage == '1901_graph' ? ratingData.tournamentDictionary.YRS
      : upsetPage ? ratingData.labelDictionary.upsetTitle
      : ratingData.teamCode[page] ? ratingData.teamDictionary[ratingData.teamCode[page]]
      : yearly ? [ ratingData.tournamentDictionary[ratingData.titleCode[page]][0] + ' ' + page ]
      : ratingData.titleCode && ratingData.titleCode[page] ? ratingData.tournamentDictionary[ratingData.titleCode[page]]
//    : ratingData.tournamentCode[page] ? ratingData.tournamentDictionary[ratingData.tournamentCode[page]]
      : ratingData.labelDictionary[page] ? ratingData.labelDictionary[page] : [ page.replace(/_/g, ' ') ];
    var prefix = ratingData.labelDictionary.mainTitle;
    var year = !yearly && ratingData.pageYear && ratingData.pageYear[page] ? ratingData.pageYear[page] + ' ' : '';
    text = new Array();
    prefix.forEach(function(pref) { text.push (pref + ': ' + year + label[0]); });
    prefix = prefix[prefix.length - 1];
    label.forEach(function(lbl) { text.push (prefix + ': ' + year + lbl); text.push (year + lbl); });
  }
//console.log('Main header ' + text.join('|'));
  var mainheader = $('#mainheader');
  var fontsize = mainheader.css('fontSize');
  var subheader = $('#subheader');
  var topnav = $('#topnav');
  var current = fixturePage ? 'fixtures' : graphPage ? 'graph' : matchPage ? 'results' : isStartingRatings() ? 'start' : isFinalRatings() || isYearEndRatings() ? 'end' : 'current';
  var currentItem = $('#topnavitem' + current);
  var higherPosition = subheader[0].getBoundingClientRect().top < 30;
//console.log('subheader top ' + subheader[0].getBoundingClientRect().top + ' higher ' + higherPosition);
//if ($(window).scrollTop() == 0)
//  higherPosition = false;
  currentItem.css('display', higherPosition ? 'none' : '');
  var topnavWidth = topnav.width(); // topnav.length > 0 ? topnav.width() : 0;
  var vw = viewportWidth();
  var layoutWidth = currentLayoutWidth();
  if (layoutWidth + 1 >= vw)
    layoutWidth -= 12;
  var availableWidth = layoutWidth - topnavWidth;
//console.log('displayHeaders topnav ' + topnavWidth + ' max ' + Math.ceil(textWidth(text[0], fontsize)) + ' available ' + availableWidth + ' text1 ' + t1.text + ' ' + t1.condensed + ' ' + t1.width + ' text2 ' + t2.text + ' ' + t2.condensed + ' ' + t2.width);
  var fulltopnav = topnavWidth > 100;
  var hastopnav = topnavWidth > 0;
//console.log('displayHeaders main ' + (fulltopnav ? layoutWidth : availableWidth) + ' ' + text.join('|'));
  text = fitHeaderText(fulltopnav ? layoutWidth : availableWidth, fontsize, text);
  if (mainheader.text() != text.text)
    mainheader.text(text.text);
  mainheader.toggleClass('condensed-header', text.condensed);
  var fit = text.width <= availableWidth;
  mainheader.css('margin-bottom', fit && hastopnav && !fulltopnav && !higherPosition ? '-36px' : fit && hastopnav && !higherPosition ? '-16px' : '16px');
  topnav.css('margin-bottom', higherPosition ? '-32px' : fit && hastopnav && !fulltopnav ? '20px' : '');
//ratingData.headerSpace = fit || !hastopnav ? 80 : 112;
  if (isLatest())
    text = ratingData.labelDictionary.latestSubtitle;
  else if (isResults())
    text = ratingData.labelDictionary.resultsSubtitle;
  else if (fixturePage)
    text = ratingData.labelDictionary.fixturesSubtitle;
  else if (graphPage)
    text = ratingData.labelDictionary.graphSubtitle;
  else if (upsetPage)
    text = ratingData.labelDictionary.upsetSubtitle;
  else if (infoPage)
    text = ratingData.labelDictionary[page + 'Subtitle'];
  else if (matchPage)
  {
    var label = ratingData.teamCode[page] ? ratingData.teamDictionary[ratingData.teamCode[page]]
//    : ratingData.tournamentCode[page] ? ratingData.tournamentDictionary[ratingData.tournamentCode[page]]
      : [ page.replace(/_/g, ' ') ];
    text = new Array();
    ratingData.labelDictionary.matchSubtitle.forEach(function(ttl)
    {
      label.forEach(function(lbl)
      {
        text.push (lbl + ' ' + ttl);
      });
    });
  }
  else
  {
    text = new Array();
    var subtitle = ratingData.labelDictionary[isFinalRatings() || isYearEndRatings() ? 'endSubtitle' : isStartingRatings() ? 'startSubtitle' : 'mainSubtitle'];
    var date = isFinalRatings() || isYearEndRatings() ? ratingData.endDate[page] : isStartingRatings() ? ratingData.startDate[page] : undefined;
    var day, month, year;
    if (date)
    {
      var dateparts = date.split('-');
      year = dateparts[0];
      month = dateparts[1];
      day = dateparts[2];
      if (day > 0)
        date = new Date(year, month - 1, day);
      else if (month > 0)
        date = new Date(year, month - 1);
      else
      {
        date = new Date(year);
        date.setFullYear(parseInt(year));
      }
    }
    else
    {
      date = ratingData.ratingAsOfDate;
      if (date)
      {
        year = date.getFullYear();
        month = date.getMonth() + 1;
        day = date.getDate();
      }
    }
    if (date)
    {
      var formatter = day > 0 ? ratingData.dateFormatter.full : month > 0 ? ratingData.dateFormatter.fullMonth : ratingData.dateFormatter.year;
      var shortDate, fullDate = formatter(date);
      if (month > 0)
      {
        formatter = day > 0 ? ratingData.dateFormatter.shorter : ratingData.dateFormatter.shorterMonth;
        shortDate = formatter(date);
      }
      ratingData.labelDictionary.dateSubtitle.forEach(function(ttl)
      {
        text.push (ttl + ' ' + fullDate);
        if (shortDate)
          text.push (ttl + ' ' + shortDate);
      });
    }
    subtitle.forEach(function(ttl)
    {
      text.push (ttl);
    });
  }
//console.log('Subheader ' + text.join('|'));
  fontsize = subheader.css('fontSize');
  layoutWidth = currentLayoutWidth();
  availableWidth = !higherPosition ? layoutWidth - 20 : vw > 400 ? layoutWidth - topnavWidth - 40 : vw > 360 ? layoutWidth - topnavWidth - 20 : layoutWidth - topnavWidth - 10;
  text = fitHeaderText(availableWidth, fontsize, text);
console.log('displaySubheader topnav ' + topnavWidth /*+ ' max ' + Math.ceil(textWidth(text[0], fontsize))*/ + ' available ' + availableWidth + ' text ' + textWidth(text.text, fontsize, text.condensed));
  if (subheader.text() != text.text)
    subheader.text(text.text);
  subheader.toggleClass('condensed-header', text.condensed);
  subheader.toggleClass('higher', higherPosition);
  $('.slick-preheader-panel').removeAttr('style');
  $('.slick-header').removeAttr('style');
  if (ratingData.scrollToTop)
  {
console.log('display headers scroll top ' + $(window).scrollTop() + ' broken firefox ' + brokenfirefox);
    $(window).scrollTop(0);
    setTimeout(resetScrollToTop, 200);
  }
}

function finishStyles()
{
  var vw = viewportWidth();
  var layoutWidth = ratingData.currentLayoutWidth;
  if (ratingData.showMainMenu)
  {
    var menuWidth = $('#accordion-menu').width();
    var spacing = ratingData.pageMargin === undefined ? 0 : vw - ratingData.pageMargin - ratingData.pageMargin - menuWidth - layoutWidth;
    if (spacing < 10)
    {
      spacing = Math.min(Math.floor((vw - menuWidth - layoutWidth) / 3), 200);
      ratingData.pageMargin = Math.floor((vw - menuWidth - layoutWidth - spacing) / 2);
      $('#main').css('margin-left', ratingData.pageMargin + 'px');
      $('#mainmenu').css('right', ratingData.pageMargin + 'px');
    }
  }
  else
  {
    $('#main').css('margin-left', '');
    delete ratingData.pageMargin;
  }
  var leaveMargins = layoutWidth + 1 < vw;
  $('#maindiv').toggleClass('verticalborders', leaveMargins);
  $('#mainheader').css('padding-left', leaveMargins ? '' : '12px');
  if (leaveMargins)
    layoutWidth += 2;
  $('#main').css('width', layoutWidth);
  $('#subheader').css('width', layoutWidth);
}

function displayInfo()
{
  var vw = viewportWidth();
  if (!ratingData.menuResizing)
    ratingData.showMainMenu = vw >= 900;
  displayMenu();
  var menuWidth = $('#accordion-menu').width();
  var layoutWidth = ratingData.showMainMenu ? Math.min(vw - menuWidth - 32, 900) : vw;
  console.log('Info viewport ' + vw + ' menu ' + menuWidth + ' available ' + layoutWidth);
  var widthUnchanged = ratingData.currentLayoutWidth == layoutWidth;
  ratingData.currentLayoutWidth = layoutWidth;
  finishStyles();
  if (widthUnchanged)
    return;
  $('#maindiv').addClass('morepadding');
  $('#maindiv').html(ratingData.dataCache[ratingData.currentPage]);
  displayTopNav();
  displayHeaders();
  $('#contactform').submit(handleSubmit);
  $('div.mainloader').addClass('invisible');
}

function handleSubmit(event)
{
  event.preventDefault();
  var formdata = $('#contactform').serialize();
  $.ajax('scripts/mail.pl', { complete: handleSubmitComplete, data: formdata, method: 'POST', success: handleSubmitResult });
}

function handleSubmitResult(data, text, xhr)
{
console.log('Submit success ' + data + ' ' + text + ' ' + ratingData.labelDictionary[data + 'Submit']);
  var form = $('#contactform');
  var formresult = $('#formresult');
  formresult.text(fitHeaderText(form.width() - 120, formresult.css('fontSize'), ratingData.labelDictionary[data + 'Submit']).text);
  if (text == 'success')
    form[0].reset();
}

function handleSubmitComplete(xhr, text)
{
console.log('Submit complete ' + text);
  if (text != 'success')
    handleSubmitResult('error', text, xhr);
}

function legendFormatter(data)
{
  if (!data.xHTML)
    return ratingData.legendText || '';
  var ds = data.xHTML.replace(/ .*/, '');
//console.log('lds: ' + ds);
  var html = ratingData.dateDate[ds];
  for (var i = 0; i < data.series.length; i++)
  {
    var series = data.series[i];
    if (series.y === undefined) continue;
    var code = series.code = ratingData.dateLabel[ds][i];
    if (ratingData.dateIncrease[ds].hasOwnProperty(code))
      series.increase = ratingData.dateIncrease[ds][code];
  }
  var srs = data.series.filter(function(s) { return s.y !== undefined; }).sort(function(a, b) { return b.y - a.y; });
  for (var i = 0; i < srs.length; i++)
  {
    var series = srs[i];
    var code = series.code;
    var increase = series.increase;
    var highlight = series.isHighlighted;
    html += '&nbsp;&nbsp; ' + (highlight ? '<b>' : '') + "<span style='color: " + series.color + ";'>" + ratingData.teamDictionary[code][0].replace(/ /g, '&nbsp;') + '</span>:&nbsp;' + (!series.hasOwnProperty('increase') ? '' : increase >= 0 ? '+' + increase + '&nbsp;' : '&minus;' + -increase + '&nbsp;') + series.yHTML + (highlight ? '</b>' : '');
  }
  return ratingData.legendText = html;
}

function dateStr(year, month, day)
{
  var ds = year;
  if (month > 0)
  {
    ds += '-';
    if (month <= 9)
      ds += '0';
    ds += month;
    if (day > 0)
    {
      ds += '-';
      if (day <= 9)
        ds += '0';
      ds += day;
    }
  }
  return ds;
}

function dateString(date)
{
  var ds = date.getFullYear();
  var month = date.getMonth() + 1;
  if (month > 0)
  {
    ds += '-';
    if (month <= 9)
      ds += '0';
    ds += month;
    var day = date.getDate();
    if (day > 0)
    {
      ds += '-';
      if (day <= 9)
        ds += '0';
      ds += day;
    }
  }
  return ds;
}

function displayGraph()
{
  var vw = viewportWidth();
  if (!ratingData.menuResizing)
    ratingData.showMainMenu = vw >= 900;
  displayMenu();
  var menuWidth = $('#accordion-menu').width();
//var layoutWidth = ratingData.showMainMenu ? Math.min(vw - menuWidth - 32, 900) : vw;
  var layoutWidth = ratingData.showMainMenu ? vw - menuWidth - 32 : vw;
  console.log('Graph viewport ' + vw + 'x' + viewportHeight() + ' menu ' + menuWidth + ' available ' + layoutWidth);
  var widthUnchanged = ratingData.currentLayoutWidth == layoutWidth;
  ratingData.currentLayoutWidth = layoutWidth;
  finishStyles();
  if (widthUnchanged)
    return;
  var basepage = basePage();
  var year, month, day, date, ds, res, row, increase, ymin = 999999, ymax = -999999;
  var dateRE = /^(\d\d\d\d)(\d\d)(\d\d)(.*)/;
  var monthRE = /^(\d\d?)(\d\d)(.*)/;
  var dayRE = /^(\d\d?)(.*)/;
  var matchRE = /^([A-Z][A-Z])([A-Z][A-Z])(-?\d+)(.*)/;
  var teamsRE = /^([A-Z][A-Z])([A-Z][A-Z])(.*)/;
  var teamRE = /^([A-Z][A-Z])(-?\d+)(.*)/;
  var labels = [ 'Rating' ];
  var rating = { };
  var index = { };
  var data = [ ];
  ratingData.dateDate = { };
  ratingData.dateLabel = { };
  ratingData.dateIncrease = { };
  ratingData.dataCache[currentPage()].split('\n').forEach(function(line)
  {
    increase = { };
    row = new Array (labels.length);
    for (var i = 0; i < labels.length; i++)
      if (index[labels[i]])
        row[i] = null;
    if ( res = dateRE.exec(line) )
    {
      year = parseInt(res[1], 10);
      month = parseInt(res[2], 10);
      day = parseInt(res[3], 10);
      line = res[4];
//console.log('date ' + year + '/' + month + '/' + day);
      date = new Date(year, month - 1, day);
      ds = Dygraph.dateString_(date).replace(/ .*/, '');
    }
    else if ( res = monthRE.exec(line) )
    {
      month = parseInt(res[1], 10);
      day = parseInt(res[2], 10);
      line = res[3];
//console.log('month ' + month + '/' + day);
      date = new Date(year, month - 1, day);
      ds = Dygraph.dateString_(date).replace(/ .*/, '');
    }
    else if ( res = dayRE.exec(line) )
    {
      day = parseInt(res[1], 10);
      line = res[2];
//console.log('day ' + day);
      date = new Date(year, month - 1, day);
      ds = Dygraph.dateString_(date).replace(/ .*/, '');
    }
//console.log('ds: ' + ds);
    while ( line )
    {
      if ( res = matchRE.exec(line) )
      {
//console.log('match ' + res[1] + ' ' + res[2] + ' ' + res[3]);
        var inc = parseInt(res[3], 10);
        var code1 = res[1];
        if (!index[code1])
        {
          index[code1] = labels.indexOf(code1);
          if (index[code1] < 0)
          {
            index[code1] = labels.length;
            row[index[code1]] = null;
            labels.push(code1);
            data.forEach(function(r)
            {
              r.push(null);
            });
          }
        }
        var code2 = res[2];
        if (!index[code2])
        {
          index[code2] = labels.indexOf(code2);
          if (index[code2] < 0)
          {
            index[code2] = labels.length;
            row[index[code2]] = null;
            labels.push(code2);
            data.forEach(function(r)
            {
              r.push(null);
            });
          }
        }
        var yval = rating[code1] += inc;
        increase[code1] = increase[code1] ? increase[code1] + inc : inc;
        row[index[code1]] = yval;
        if ( yval > ymax )
          ymax = yval;
        else if ( yval < ymin )
          ymin = yval;
        yval = rating[code2] -= inc;
        increase[code2] = increase[code2] ? increase[code2] - inc : - inc;
        row[index[code2]] = yval;
        if ( yval > ymax )
          ymax = yval;
        else if ( yval < ymin )
          ymin = yval;
        line = res[4];
      }
      else if ( res = teamsRE.exec(line) )
      {
        var code1 = res[1];
        var code2 = res[2];
//console.log('teams ' + code1 + ' ' + code2);
        if (code2 == 'XX')
        {
          delete index[code1];
        }
        else
        {
          index[code2] = index[code1];
          rating[code2] = rating[code1];
          labels[index[code2]] = code2;
        }
        line = res[3];
      }
      else if ( res = teamRE.exec(line) )
      {
//console.log('team ' + res[1] + ' ' + res[2]);
        var code = res[1];
        if (!index[code])
        {
          index[code] = labels.indexOf(code);
          if (index[code] < 0)
          {
            index[code] = labels.length;
            row[index[code]] = null;
            labels.push(code);
            data.forEach(function(r)
            {
              r.push(null);
            });
          }
        }
        var inc = parseInt(res[2], 10);
        var yval;
        if (rating.hasOwnProperty(code))
        {
          yval = rating[code] += inc;
          increase[code] = increase[code] ? increase[code] + inc : inc;
          row[index[code]] = yval;
        }
        else
        {
//console.log('start ' + code + ' ' + inc + ' ' + labels.length);
          yval = rating[code] = inc;
          var startDate = ratingData.startDate[basepage];
//console.log('startDate ' + startDate);
          if (data.length == 0 && startDate)
          {
            date = new Date(startDate.replace(/-/g, '/'));
            year = date.getFullYear();
            month = date.getMonth() + 1;
            day = date.getDate();
            ds = Dygraph.dateString_(date).replace(/ .*/, '');
            row[index[code]] = yval;
//console.log('startDate ' + date);
          }
        }
        if ( yval > ymax )
          ymax = yval;
        else if ( yval < ymin )
          ymin = yval;
        line = res[3];
      }
      else
      {
        console.log('line ' + line);
        return;
      }
    }
    if ( date )
    {
      if (data.length == 0)
        for (var i = 1; i < labels.length; i++)
          row[i] = rating[labels[i]];
//console.log('row ' + row);
      row[0] = date;
      data.push(row);
      ratingData.dateDate[ds] = dateStr(year, month, day);
      ratingData.dateLabel[ds] = labels.slice(1);
      ratingData.dateIncrease[ds] = increase;
    }
  });
  var endDate = ratingData.endDate[basepage];
//console.log('ed ' + endDate + ' raod ' + ratingData.ratingAsOfDate);
  endDate = endDate ? new Date(endDate.replace(/-/g, '/')) : ratingData.ratingAsOfDate;
  var eds = Dygraph.dateString_(endDate).replace(/ .*/, '');
//console.log('ds ' + ds + ' eds ' + eds);
//console.log('ed ' + endDate);
  if (ds != eds)
  {
    row = Array (labels.length);
    row[0] = endDate;
    data.push(row);
    ratingData.dateDate[eds] = dateString(endDate);
    ratingData.dateLabel[eds] = labels.slice(1);
    ratingData.dateIncrease[eds] = { };
  }
  for (var i = 1; i < labels.length; i++)
    if (index[labels[i]])
      row[i] = rating[labels[i]];
//console.log('labels ' + labels);
//console.log('today ' + row);
  ymin = Math.floor((ymin - 10) / 50) * 50;
  ymax = Math.ceil((ymax + 10) / 50) * 50 + 1;
  $('#maindiv').toggleClass('morepadding', ratingData.showMainMenu);
  $('#maindiv').html("<div id='maingraph'></div><div id='graphlegend'></div>");
  var graphlegend = $('#graphlegend');
  var availableWidth = layoutWidth;
  if (ratingData.showMainMenu)
    availableWidth -= 32;
  var availableHeight = viewportHeight() - (ratingData.showMainMenu ? 60 : 54);
  if (availableHeight < 600)
    graphlegend.addClass('smaller');
console.log('graph height ' + graphlegend.height() + ' available ' + availableHeight);
  availableHeight -= graphlegend.height();
  if (availableHeight > availableWidth)
    availableHeight = availableWidth;
  new Dygraph(document.getElementById('maingraph'), data, {
    //axes: { y2: { } },
    axes: { x: { axisLabelWidth: 64 }, y: { axisLabelWidth: 32 } },
    connectSeparatedPoints: true,
    highlightSeriesOpts: { strokeWidth: 1 },
//  interactionModel: Dygraph.defaultInteractionModel,
    interactionModel: interactionModel = {
      mousedown: Dygraph.defaultInteractionModel.mousedown,
      dblclick: Dygraph.defaultInteractionModel.dblclick,
      touchstart: function(event, g, context)
      {
        Dygraph.defaultInteractionModel.touchstart(event, g, context);
        context.touchDirections = { x: false, y: false };
      },
      touchend: Dygraph.defaultInteractionModel.touchend, 
      touchmove: Dygraph.defaultInteractionModel.touchmove,
      willDestroyContextMyself: true
    },
    labels: labels,
    labelsDiv: 'graphlegend',
    legendFormatter: legendFormatter,
    height: availableHeight,
    panEdgeFraction: 0,
    rightGap: 10,
    //showLabelsOnHighlight: false,
    stepPlot: true,
    strokeWidth: 0.7,
    valueRange: [ymin, ymax],
    width: availableWidth,
    xRangePad: 5
  });
  displayTopNav();
  displayHeaders();
  $('div.mainloader').addClass('invisible');
}
/*
ticker: function(a, b, pixels, options_view, dygraph, forced_values) {
                        var dt0 = getDateTime(a);
                        var startLabel = dt0['date'] + "\n" + dt0['time'];
                        var dt1 = getDateTime(b);
                        var endLabel = dt1['date'] + "\n" + dt1['time'];
                        return [ {v: a, label: startLabel}, {v: b, label: endLabel} ];
                    }
*/

function displayTable()
{
  var page = ratingData.currentPage;
  var pageData = currentPageData();
  pageData.hideGroups.forEach(hideVerticalLine);
  //layoutGroups(pageData.groups);
  //createLayout();
  if (calculateWidth())
  {
    var lines = ratingData.dataCache[page].split('\n');
    ratingData.dataArray = new Array();
    lines.forEach(pageData.pushRow);
    renderTable();
  }
  displayTopNav();
  displayHeaders();
  $('div.mainloader').addClass('invisible');
}

function cellFormatter(row, cell, value, columnDef, dataContext)
{
  return value == null ? "" : value + "";
}

function renderTable()
{
  var page = ratingData.currentPage;
  var pageData = currentPageData();
  console.log('render table ' + page);
  var data = ratingData.dataArray;
  var tableid = 'maintable_' + page;
  var tableselector = '#' + tableid;
  $(tableselector).attr('id', tableid + '_remove');
  var maindiv = $('#maindiv');
  var remove = maindiv.children();
  maindiv.append("<div id='" + tableid + "' class='maintable'></div>");
  var table = $(tableselector);
  table.css('width', ratingData.currentLayoutWidth);
  var tableTime = Date.now();
  var options = {
    autoHeight: true,
    createPreHeaderPanel: ratingData.showGroupHeaders,
    defaultFormatter: cellFormatter,
    //enableCellNavigation: true,
    enableColumnReorder: false,
    enableTextSelectionOnCells: true,
    //headerRowHeight: (ratingData.showGroupHeaders ? 35 : 60),
    preHeaderPanelHeight: 35,
    rowHeight: ratingData.currentLayout == 'condensed' ? pageData.condHeight : pageData.rowHeight,
    showPreHeaderPanel: ratingData.showGroupHeaders
  };
  var columns = ratingData.columnLayout;
  var grid = new Slick.Grid(tableselector, data, columns, options);
  $('.slick-header').toggleClass('full-header', ratingData.showGroupHeaders);
  $('.slick-header-columns').width(ratingData.currentLayoutWidth);
  if (ratingData.showGroupHeaders)
  {
    var preHeaderPanel = $(grid.getPreHeaderPanel())
      .addClass('slick-header-columns')
      .width(ratingData.currentLayoutWidth);
    var headerColumnWidthDiff = grid.getHeaderColumnWidthDiff();
    pageData.groups.forEach(function(group)
    {
      if (group.visible)
      {
        var widthTotal = 0;
        group.columns.forEach(function(column)
        {
          widthTotal += column.layout.width;
        });
        $("<div class='ui-state-default slick-header-column group-header'/>")
          .html("<span class='slick-column-name'>" + (group.layout && group.layout.name ? group.layout.name : '') + "</span>")
          .width(widthTotal - headerColumnWidthDiff)
          .appendTo(preHeaderPanel);
      }
    });
  }
  console.log('Table created in ' + (Date.now() - tableTime) + ' ms');
  table.css('overflow', 'visible');
  remove.remove();
  displayVerticalLines();
}

function appendMenuItems(container, items)
{
  items.forEach(function(item)
  {
//console.log('item ' + item.id + ' page ' + item.page + ' label ' + item.label);
    if (item.class)
      container.addClass(item.class);
//  if (!item.page)
//    item.page = item.id;
    var li = $('<li id="item' + item.id + '"' + (item.class ? ' class="' + item.class + '"' : '') + '></li>').appendTo(container);
    if (item.items)
    {
      if (item.page)
        li.data('page', item.page);
      var cb = $("<input type='checkbox' name='" + item.id + "' id='" + item.id + "'/>");
      cb.change(menuCheckboxChange);
      cb.appendTo(li);
      $("<label for='" + item.id + "'>" + item.label[item.label.length - 1].replace(/ /g, '&nbsp;') + "</label>").appendTo(li);
      var ul = $('<ul></ul>').appendTo(li);
      appendMenuItems(ul, item.items);
    }
    else if (item.html)
    {
      var span = $('<span class="accordion" onclick="handleClick(\'' + item.id + '\')">' + item.html + '</span>');
      span.appendTo(li);
    }
    else
    {
      var page = item.page || item.id;
      li.data('page', page);
//console.log('appending ' + page);
      var a = $('<a href="' + (page == 'World' ? './' : page) + '"' + (page.indexOf('/') == 0 ? '' : ' onclick="handleLink(\'' + page + '\'); return false;"') + '>' + item.label[item.label.length - 1].replace(/ /g, '&nbsp;') + '</a>');
      a.appendTo(li);
    }
  });
}

function createMenu()
{
  var menu = $('#accordion-menu');
  if (menu.length > 0)
    return;
//console.log('Creating menu');
  var hockey = isHockey();
  var menuitems = new Array();
  menuitems.push({ id: 'pr1', label: ratingData.labelDictionary.resultsMenuLabel });
  if (!hockey)
    menuitems.push({ id: 'pr2', label: ratingData.labelDictionary.resultsMenuLabel });
  var items = new Array();
//if (!hockey)
//{
    var levels = new Array();
    ratingData.titleCode = { };
    ratingData.pageYear = { };
    ratingData.startDate = { };
    ratingData.endDate = { };
    ratingData.tournamentPage = { };
    var ids = { };
    ratingData.dataCache.menu.split('\n').forEach(function(line)
    {
//console.log('line ' + line);
      var fields = line.split('\t');
      var page = fields[5];
      var title = ratingData.titleCode[page] = fields[0];
      var level = fields[1];
      var label = fields[2];
      if (!label)
        label = title;
      var year;
      var clss;
      if (/^\d\d\d\d$/.exec(label) || /^\d\d\d\d[-−‒–]\d\d?$/.exec(label) || /^\d\d\d\d_II?$/.exec(label))
      {
        label = label.replace(/_/, ' ');
        year = label.replace(/ II?/, '');
        clss = 'inline';
      }
      else
      {
console.log(label);
        label = ratingData.tournamentDictionary[label][0];
        var yearmatch = /^\d\d\d\d(?:-\d+)/.exec(page);
        if (yearmatch)
          year = yearmatch[0];
        else if (level > 0)
          year = ratingData.pageYear[levels[level - 1].page];
      }
      if (year)
        ratingData.pageYear[page] = year;
      var startDate = ratingData.startDate[page] = fields[3];
      var endDate = ratingData.endDate[page] = fields[4];
      if (!ratingData.tournamentPage.hasOwnProperty(title))
        ratingData.tournamentPage[title] = new Array();
      ratingData.tournamentPage[title].unshift({ page: page, startDate: startDate, endDate: endDate });
      var itemlist = menuitems;
      if (level > 0)
      {
        var parent = levels[level - 1];
        if (!parent.items)
          parent.items = [ ];
        itemlist = parent.items;
      }
      var id = !page ? 'dropdown' + label.replace(/[-−‒– ]/, '') : level == 0 ? page.replace(/^\d\d\d\d_/, '').replace(/_A$/, '') : page;
      var idno = ids[id] || '';
      if (idno)
        ids[id] ++;
      else
        ids[id] = 1;
      itemlist.push(levels[level] = { id: id + idno, page: page, class: clss, label: [ label ] });
//label = label[0].toUpperCase() + label.substr(1);
    });
//menuitems.push({ id: 'dropdownTournaments', label: ratingData.labelDictionary.tournamentsMenuLabel, items: items });
    items = new Array();
//}
  menuitems.push({ id: 'upsets', label: ratingData.labelDictionary.upsetsMenuLabel });
  if (!hockey)
    menuitems.push({ id: 'matchlist', page: '/matches/', label: ratingData.labelDictionary.matchListMenuLabel });
  items = new Array();
  ratingData.menuAllGroups.forEach(function(group)
  {
    var labels = ratingData.labelDictionary[group.id + 'MenuLabel'] || ratingData.labelDictionary[group.id + 'Header'];
    items.push({ id: 'group' + group.id, html: makeCheckbox('checkmark' + group.id, labels[0].replace(/ /g, '&nbsp;')) });
  });
  if (hockey)
    menuitems.push({ id: 'football', page: '/', label: ratingData.labelDictionary.footballMenuLabel });
  else
//{
//  menuitems.push({ id: 'news', page: '//football-headlines.com/', label: ratingData.labelDictionary.newsMenuLabel });
    menuitems.push({ id: 'hockey', page: '/icehockey/', label: ratingData.labelDictionary.hockeyMenuLabel });
//}
  menuitems.push({ id: 'dropdownColumns', label: ratingData.labelDictionary.columnsMenuLabel, items: items });
  menuitems.push({ id: 'about', label: ratingData.labelDictionary.aboutMenuLabel });
  menuitems.push({ id: 'contact', label: ratingData.labelDictionary.contactMenuLabel });
//menuitems.push({ id: 'recommended', label: ratingData.labelDictionary.recommendedMenuLabel });
  menuitems.push({ id: 'donate', page: 'https://ko-fi.com/eloratings', label: ratingData.labelDictionary.donateMenuLabel });
  var menu = $('<ul id="accordion-menu" class="accordion-menu"></ul>');
  ratingData.expandedItems = new Array();
  appendMenuItems(menu, menuitems);
  menu.appendTo($('#mainmenu'));
//console.log('Menu width ' + menu.width() + ' height ' + menu.height());
  ratingData.finishMenuLater = true;
  return menu;
}

function fitMenuToViewport(expandingItem)
{
//console.log('fmtvp ' + ratingData.menuResizing);
  if (ratingData.menuResizing)
    return;
  var keepVisibleItem = expandingItem || ratingData.linkItem;
  var addedHeight = expandingItem ? expandingItem.children('ul').height() : 0;
  var menu = $('#accordion-menu');
  var vh = viewportHeight();
  var bottom = menu.height();
  if (!bottom || !menu[0])
    return;
//if (!ratingData.showMainMenu)
//  bottom += menu[0].getBoundingClientRect().top;
  var overflow = bottom + addedHeight - vh;
console.log('Body ' + parseFloat(window.getComputedStyle(document.body).height) + ' window ' + $(window).height() + ' inner ' + window.innerHeight);
console.log('Accordion bottom ' + bottom + ' added height ' + addedHeight + ' viewport height ' + vh + ' overflow ' + overflow);
if (expandingItem) console.log(' expanding item ' + expandingItem.prop('id'));
if (ratingData.linkItem) console.log(' keep visible item ' + ratingData.linkItem.prop('id'));
  var i = 0;
  while (overflow > 76 && i < ratingData.expandedItems.length)
  {
    var id = ratingData.expandedItems[i];
console.log('item ' + id);
    var ul = menu.find('#' + id).children('ul');
    if (keepVisibleItem && keepVisibleItem.closest('#' + id).length > 0 || ul.is(':hidden'))
      i++;
    else
    {
      var height = ul.height();
console.log(' height ' + height + ' overflow ' + overflow);
      if (height < overflow && i < ratingData.expandedItems.length - 1)
      {
        var id2 = ratingData.expandedItems[i + 1];
        var ul2 = menu.find('#' + id2).children('ul');
        var ht2 = ul2.height();
console.log(' next ' + id2 + ' height ' + ht2);
        if ((ht2 >= overflow || /*FIX*/ ul.closest('#' + id2).length > 0) && (!keepVisibleItem || keepVisibleItem.closest('#' + id2).length == 0))
        {
          ul = ul2;
          height = ht2;
          i++;
        }
      }
      var cb = ul.siblings('input[type="checkbox"]');
      cb.prop('checked', false);
      overflow -= height;
      ratingData.expandedItems.splice(i, 1);
      if (expandingItem)
        ul.slideUp(300);
      else
        ul.css('display', 'none');
      ul.parent().removeClass('expanded');
    }
  }
  var hide = keepVisibleItem && overflow > 76;
  var extra_overflow = 0;
  menu.children('li').each(function()
  {
    var li = $(this);
    if (hide && !li.children('input[type="checkbox"]').prop('checked') || li.data('hide'))
    {
      if (li.css('display') != 'none')
        overflow -= 41;
      li.css('display', 'none');
//console.log('fmtvp hide ' + li.prop('id'));
    }
    else
    {
      if (li.css('display') == 'none')
        extra_overflow += 41;
    }
  });
  if (!hide && overflow + extra_overflow <= 76)
    menu.children('li').each(function()
    {
      var li = $(this);
      if (!li.data('hide'))
        li.css('display', '');
//if (!li.data('hide')) console.log('fmtvp show ' + li.prop('id'));
    });
  if (ratingData.showMainMenu)
  {
    var tp = (overflow >= 0 ? 0 : overflow <= -94 ? 47 : - overflow / 2) + 'px';
    $('#mainmenu').css('padding-top', tp);
console.log('tp ' + tp + ' overflow ' + overflow);
  }
  ratingData.currentViewportHeight = vh;
}

function menuCheckboxChange()
{
  var checkbox = $(this);
  var checked = checkbox.prop('checked');
  var li = checkbox.parent();
  var ul = checkbox.siblings('ul');
  var id = li[0].id;
  var link = li.data('page');
  if (link == ratingData.currentPage)
    link = undefined;
  if (link)
    ratingData.linkItem = li;
  li.toggleClass('expanded', checked);
  menuHideDelay = 2000;
  setTimeout(resetMenuHideDelay, 2000);
  if (checked)
  {
    if (!link)
      fitMenuToViewport(li);
console.log('expanding ' + id + ' height ' + ul.height() + ' menu width ' + $('#accordion-menu').width());
    ratingData.expandedItems.push(id);
    ratingData.menuResizing = true;
    ul.slideDown(300, menuSlideComplete);
  }
  else
  {
console.log('collapsing ' + id + ' menu width ' + $('#accordion-menu').width());
    var index = ratingData.expandedItems.indexOf(id);
    if (index >= 0)
      ratingData.expandedItems.splice(index, 1);
    $('#accordion-menu').children('li').each(function()
    {
      var li = $(this);
      li.css('display', li.data('hide') ? 'none' : '');
    });
    ratingData.menuResizing = true;
    ul.slideUp(300, menuSlideComplete);
  }
  if (link)
    handleLink(link);
}

function menuSlideComplete()
{
console.log('slide complete: menu width ' + $('#accordion-menu').width());
  if (ratingData.showMainMenu && !ratingData.linkItem)
    displayPage();
  delete ratingData.menuResizing;
  if (ratingData.linkItem)
    displayPage();
}

function updateMenu(menu)
{
  var page = ratingData.currentPage;
  var item = menu.find('#itempr1');
  if (page == 'East_Germany' || page == 'Germany' || page == 'North_Vietnam' || page == 'North_Yemen' || page == 'Saar' || page == 'Vietnam' || page == 'Yemen')
  {
    var id = page == 'Germany' ? 'East_Germany' : page == 'North_Vietnam' ? 'Vietnam' : page == 'North_Yemen' ? 'Yemen' : page == 'Vietnam' ? 'North_Vietnam' : page == 'Yemen' ? 'North_Yemen' : 'Germany';
    var a = item.children('a');
    a.attr('href', id);
    a.attr('onclick', 'handleLink(\'' + id + '\'); return false;');
    a.text(ratingData.teamDictionary[page == 'Germany' ? 'DD' : page == 'North_Vietnam' ? 'VN' : page == 'North_Yemen' ? 'YE' : page == 'Vietnam' ? 'NV' : page == 'Yemen' ? 'NY' : 'DE'][0]);
    item.css('display', '');
    item.removeData('hide');
  }
  else
  {
    item.css('display', 'none');
    item.data('hide', true);
  }
  if (!isHockey())
  {
    item = menu.find('#itempr2');
    if (page == 'Germany')
    {
      var a = item.children('a');
      a.attr('href', 'Saar');
      a.attr('onclick', 'handleLink(\'Saar\'); return false;');
      a.text(ratingData.teamDictionary['SP'][0]);
      item.css('display', '');
      item.removeData('hide');
    }
    else
    {
      item.css('display', 'none');
      item.data('hide', true);
    }
  }
  var c = menu.find('#itemdropdownColumns');
  if (isInfo())
  {
    c.css('display', 'none');
    c.data('hide', true);
  }
  else
  {
    //c.css('display', '');
    c.removeData('hide');
console.log('um show ' + c.prop('id'));
  }
  updateMenuItems(menu);
}

function finishMenu(menu)
{
  if (!ratingData.finishMenuLater)
    return;
  var bottom = menu.height();
  if (!bottom)
    return;
  delete ratingData.finishMenuLater;
  var vh = viewportHeight();
//if (!ratingData.showMainMenu)
//  bottom += $('#topnavitemdropdown')[0].getBoundingClientRect().bottom;
  var item = menu.find('#itemWorld_Cup');
  var list = item.children('ul');
  var checkbox = item.children('input');
  var checked = checkbox.prop('checked');
//console.log('World Cup list height ' + list.height() + ' menu ' + menu.height() + ' bottom ' + bottom + ' viewport ' + vh);
  if (!checked && bottom + list.height() < vh + 5)
  {
    item.addClass('expanded');
    checkbox.prop('checked', true);
    list.css('display', 'block');
    ratingData.expandedItems.push('itemWorld_Cup');
    bottom += item.height();
  }
  item = menu.find('#itemOlympic_Games');
  list = item.children('ul');
  checkbox = item.children('input');
  checked = checkbox.prop('checked');
//console.log('ENL list height ' + list.height() + ' menu ' + menu.height() + ' bottom ' + bottom + ' viewport ' + vh);
  if (!checked && bottom + list.height() < vh + 5)
  {
    item.addClass('expanded');
    item.children('input').prop('checked', true);
    list.css('display', 'block');
    ratingData.expandedItems.push('itemOlympic_Games');
  }
}

function updateMenuItems(menu)
{
  var result = false;
  var page = basePage();
  menu.children('li').each(function()
  {
    var li = $(this);
    var ul = li.children('ul');
    if (ul.length > 0)
    {
      var id = li.prop('id');
      var current = li.data('page') == page;
      var label = li.children('label');
      label.toggleClass('current', current);
      if (current)
        result = true;
      var cb = li.children('input');
      var ok = ratingData.expandedItems.length == 0 && !cb.prop('checked');
      if (updateMenuItems(ul) && ok)
      {
        li.addClass('expanded');
        cb.prop('checked', true);
        ul.css('display', 'block');
        ratingData.expandedItems.push(id);
        result = true;
      }
      //else if (cb.prop('checked'))
      //  ratingData.expandedItems.push(id);
    }
    else
    {
      var a = li.children('a');
      var current = a.attr('href') == (page == 'World' ? './' : page);
      a.toggleClass('current', current);
      if (current)
        result = true;
    }
  });
  return result;
}

var menuShown = false, showingMenu = false, hidingMenu = false, ignoreDropdownClick = false;
var menuHideDelay = 200;

function resetMenuHideDelay()
{
  menuHideDelay = 200;
}

function clearDropdownTimeout()
{
  if (ratingData.dropdownTimeout)
  {
    clearTimeout(ratingData.dropdownTimeout);
    delete ratingData.dropdownTimeout;
  }
  hidingMenu = showingMenu = false;
}

function enableDropdownClick()
{
  ignoreDropdownClick = false;
}

function delayedShowMenu(timeout)
{
  showingMenu = true;
  ignoreDropdownClick = true;
  setTimeout(enableDropdownClick, 500);
  ratingData.dropdownTimeout = setTimeout(showMenu, timeout);
}

function delayedHideMenu(timeout)
{
  hidingMenu = true;
  ratingData.dropdownTimeout = setTimeout(hideMenu, timeout);
}

function showMenu()
{
  var menu = $('#accordion-menu');
  console.log('Show menu: menu shown ' + menuShown + ' showing ' + showingMenu + ' hiding ' + hidingMenu + ' menu ' + menu.length);
  delete ratingData.dropdownTimeout;
  showingMenu = false;
  menuShown = true;
  menu.children('li').each(function()
  {
    var li = $(this);
    if (!li.data('hide'))
      li.css('display', '');
if (!li.data('hide')) console.log('sm show ' + li.prop('id'));
  });
  $('#topmenu').removeClass('invisible');
  finishMenu(menu);
  fitMenuToViewport();
}

function hideMenu()
{
  console.log('Hide menu: menu shown ' + menuShown + ' showing ' + showingMenu + ' hiding ' + hidingMenu);
  delete ratingData.dropdownTimeout;
  hidingMenu = menuShown = false;
  $('#topmenu').addClass('invisible');
}

function dropdownClick()
{
  console.log('Dropdown click: menu shown ' + menuShown + ' showing ' + showingMenu + ' hiding ' + hidingMenu);
  if (ignoreDropdownClick)
    return;
  clearDropdownTimeout();
  menuShown ? delayedHideMenu() : delayedShowMenu();
  return false;
}

function menuEnter()
{
  console.log('Dropdown enter: menu shown ' + menuShown + ' showing ' + showingMenu + ' hiding ' + hidingMenu);
  if (hidingMenu)
    clearDropdownTimeout();
  if (!menuShown && !showingMenu)
    delayedShowMenu();
}

function menuLeave(event)
{
  console.log('Dropdown leave: menu shown ' + menuShown + ' showing ' + showingMenu + ' hiding ' + hidingMenu);
  // Chrome sends false leave events when expanding/collapsing submenus
  var tm = $('#topmenu');
  var offset = tm.offset();
//console.log(' page X ' + event.pageX + ' Y ' + event.pageY + ' offset ' + offset.left + ' ' + (offset.left + tm.width()) + ' ' + offset.top + ' ' + (offset.top + tm.height()));
  if (event.pageX > Math.ceil(offset.left) && event.pageX < Math.floor(offset.left + tm.width()) && event.pageY > Math.ceil(offset.top) && event.pageY < Math.floor(offset.top + tm.height()))
    return;
  // end Chrome workaround
  if (showingMenu)
    clearDropdownTimeout();
  if (menuShown && !hidingMenu)
    delayedHideMenu(menuHideDelay);
}

function displayTopNav()
{
  var page = ratingData.currentPage;
  var basepage = basePage();
  var navitems = new Array();
  if (ratingData.startDate[basepage])
    navitems.push({ id: 'start', href: startRatings(), label: ratingData.labelDictionary.startMenuLabel });
  if (isCurrentRatings() || isLatest() || isFixtures() || isCurrentGraph())
  {
    if (ratingData.startDate[basepage])
      navitems.push({ id: 'current', href: currentRatings(), label: ratingData.labelDictionary.curMenuLabel });
    else
      navitems.push({ id: 'current', href: currentRatings(), label: ratingData.labelDictionary.ratingsMenuLabel });
    navitems.push({ id: 'results', href: currentResults(), label: ratingData.labelDictionary.resultsMenuLabel });
    var fixtures = currentFixtures();
    if (ratingData.titleCode[basepage] && !ratingData.endDate[basepage])
      navitems.push({ id: 'fixtures', href: fixtures, label: ratingData.labelDictionary.fixturesMenuLabel });
  }
  else if (isPastRatings() || isResults() || isGraph())
  {
    if (ratingData.endDate[basepage])
      navitems.push({ id: 'end', href: pastRatings(), label: ratingData.labelDictionary.endMenuLabel });
    else
      navitems.push({ id: 'current', href: pastRatings(), label: ratingData.labelDictionary.curMenuLabel });
    navitems.push({ id: 'results', href: pastResults(), label: ratingData.labelDictionary.resultsMenuLabel });
  }
  if (isGlobal() || isRegion() || isConfederation() || isLatest() || isResults() || isFixtures() || isGraph() || isTournament() && /^\d\d\d\d/.exec(page))
    navitems.push({ id: 'graph', href: currentGraph(), label: ratingData.labelDictionary.graphMenuLabel });
  if (navitems.length > 0)
    navitems[navitems.length - 1].class = 'lastnav';
  ratingData.topnavWidth = 0;
  var navindex = -1, navcond = false, navtight = false;
  if (navitems.length)
  {
    ratingData.topnavWidth = 1;
    var labelCount = navitems[0].label.length;
    var layoutWidth = ratingData.currentLayoutWidth;
    var navwidth;
//console.log('topnav layout width ' + layoutWidth + ' main menu ' + ratingData.showMainMenu);
    do
    {
      navindex ++;
      if (navindex >= labelCount)
      {
        if (navcond)
        {
          navindex = labelCount - 1;
          navtight = true;
          break;
        }
        else
        {
          navindex = 0;
          navcond = true;
        }
      }
      navwidth = ratingData.showMainMenu ? 1 : 55;
      navitems.forEach(function(item)
      {
//      if (labelCount > item.label.length)
//        labelCount = item.label.length;
//      if (navindex >= labelCount)
//        return;
        navwidth += Math.ceil(textWidth(item.label[navindex], '14pt', navcond)) + (navcond ? 17 : 25);
      });
    }
    while (navwidth > layoutWidth);
//console.log('topnav index ' + navindex + ' / ' + labelCount);
  }
  if (!ratingData.showMainMenu)
    navitems.push({ id: 'dropdown', class: 'dropdown', html: "<div class='menuicon'></div>" /*, label: '☰'*/ });
  var menu = $('#topmenu > #accordion-menu').detach();
  var topnav = $('#topnav').empty();
  if (navitems.length)
  {
    var ul = $('<ul></ul>').appendTo(topnav);
    navitems.forEach(function(item)
    {
      var dropdown = item.id == 'dropdown';
      var li = $('<li id="topnavitem' + item.id + '" class="topnavitem"></li>').appendTo(ul);
      var a = dropdown ? $(item.html /* '<span>' + label + '</span>' */) : $('<a href="' + item.href + '" onclick="handleLink(\'' + item.href + '\'); return false;">' + item.label[navindex] + '</a>');
      if (item.class)
        li.addClass(item.class);
      if (item.href == (page == 'World' ? './' : page))
        a.addClass('current');
      if (navtight)
        a.addClass('tighter');
      else if (navcond)
        a.addClass('condensed-header');
      a.appendTo(li);
      var width = dropdown ? 54 : Math.ceil(textWidth(item.label[navindex], '14pt') + 21);
      if (!dropdown)
        ratingData['topnavitem' + item.id + 'Width'] = width;
      ratingData.topnavWidth += width;
//console.log('topnav item ' + item.id + ' width ' + width);
    });
  }
console.log('topnav width ' + ratingData.topnavWidth);
  if (!ratingData.showMainMenu)
  {
console.log('topnav menu shown ' + menuShown);
    var holder = $('<div id="menuholder" class="menuholder"></div>').appendTo(topnav);
    var topmenu = $('<div id="topmenu" class="topmenu' + (menuShown ? '' : ' invisible') + '"></div>').appendTo(holder);
    if (menu.length == 0)
      menu = $('#accordion-menu').detach();
    menu.appendTo(topmenu);
    var dropdown = $('#topnavitemdropdown');
    dropdown.click(dropdownClick);
    dropdown.hover(menuEnter, menuLeave);
    topmenu.hover(menuEnter, menuLeave);
  }
  if (menuShown || ratingData.showMainMenu)
    fitMenuToViewport();
  if (!ratingData.menuResizing)
    delete ratingData.linkItem;
}

function displayMenu()
{
  if (ratingData.menuResizing)
    return;
  var menu = $('#accordion-menu');
  var mainmenu = $('#mainmenu');
  mainmenu.toggleClass('invisible', !ratingData.showMainMenu);
  updateMenu(menu);
  updateColumnMenu();
  if (ratingData.showMainMenu)
  {
    menu.detach();
    menu.appendTo(mainmenu);
    finishMenu(menu);
    fitMenuToViewport();
  }
//console.log('Menu width ' + menu.width() + ' height ' + menu.height());
}

function updateColumnMenu()
{
  var groups = isFixtures() ? ratingData.menuFixtureHideGroups : isMatches() ? ratingData.menuMatchHideGroups : ratingData.menuRatingHideGroups;
  groups.forEach(function(group)
  {
    $('#itemgroup' + group.id).css('display', 'none');
  });
  groups = isFixtures() ? ratingData.menuFixtureGroups : isMatches() ? ratingData.menuMatchGroups : isFinalRatings() && ! isYearEndRatings() || isCurRatings() ? ratingData.menuFinalGroups : ratingData.menuRatingGroups;
  groups.forEach(function(group)
  {
    $('#itemgroup' + group.id).css('display', '');
    $('#checkmark' + group.id).css('visibility', group.priority <= ratingData.includedPriority ? 'visible' : 'hidden');
  });
}

function setupScrolling()
{
  handleScroll();
  $(window).on("scroll touchmove", handleScroll);
}

function handleScroll()
{
//console.log('scroll ' + $(window).scrollTop());
  if (ratingData.scrollToTop)
  {
//console.log('scroll event scroll top ' + $(window).scrollTop() + ' broken firefox ' + brokenfirefox);
    delete ratingData.scrollToTop;
    $(window).scrollTop(0);
    if (brokenfirefox)
      displayPage(true);
  }
  displayHeaders();
}

function resetScrollToTop()
{
console.log('reset scroll to top');
  delete ratingData.scrollToTop;
}

/*
var scrollPosition = 0;
var previousScrollPosition = 0;
var higherPosition = false;

function handleScroll()
{
//var scrollTop = function(){ return (window.pageYOffset !== undefined) ? window.pageYOffset : (document.documentElement || document.body.parentNode || document.body).scrollTop; }
  var pos = $(window).scrollTop();
  if ( pos != scrollPosition )
  {
    higherPosition = pos > 0 && pos > scrollPosition || pos >= ratingData.headerSpace && pos < scrollPosition || pos > ratingData.headerSpace - 20 && pos == scrollPosition;
//console.log('scroll higher ' + higherPosition + ' position ' + pos + ' ' + scrollPosition + ' ' + previousScrollPosition);
    //document.getElementById('subheader').className = higherPosition ? 'subheader higher' : 'subheader';
    previousScrollPosition = scrollPosition;
    scrollPosition = pos;
    if (ratingData.scrollTimeoutID)
      clearTimeout(ratingData.scrollTimeoutID);
      //cancelAnimationFrame(ratingData.scrollTimeoutID);
    //ratingData.scrollTimeoutID = requestAnimationFrame(checkScroll);
    ratingData.scrollTimeoutID = setTimeout(checkScroll, 50);
  }
}

function checkScroll()
{
  delete ratingData.scrollTimeoutID;
  var pos = $(window).scrollTop();
console.log('check scroll ' + higherPosition + ' position ' + pos + ' ' + scrollPosition + ' ' + previousScrollPosition);
//if (higherPosition && pos < ratingData.headerSpace || previousScrollPosition == 0 && pos < ratingData.headerSpace + 40)
//  $(window).scrollTop(ratingData.headerSpace);
//else if (!higherPosition && pos > 0)
//  $(window).scrollTop(0);
  displayHeaders();
}
*/
