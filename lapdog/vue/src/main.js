import Vue from 'vue'
import App from './App.vue'
import Router from 'vue-router'
import Home from './Pages/Home.vue'
import Workspace from './Pages/Workspace.vue'
import Submission from './Pages/Submission.vue'
import Methods from './Pages/Methods.vue'
import Pagination from './Components/Pagination.vue'
import Preview from './Components/Preview.vue'
// import Slideout from 'vue-slideout'
window.$ = require('jquery')
window.jQuery = require('jquery')
window.materialize = require('materialize-css')
Vue.use(Router)

// require('dotenv').config()

const router = new Router({
  routes:
  [
    {
      path: '/',
      name: 'home',
      component: Home,
    },
    {
      path: '/workspaces/:namespace/:workspace',
      name: 'workspace',
      component: Workspace,
      props: true
    },
    {
      path: '/workspaces/:namespace/:workspace/submissions/:submission_id',
      name: 'submission',
      component: Submission,
      props: true
    },
    {
      path: '/workspaces/:namespace/:workspace/methods',
      name: 'methods',
      component: Methods,
      props: true
    },
    {
      path: '*',
      redirect: '/'
    }
  ]
})

// Vue.component('sidenav', {
//   template: ,
//   mounted: () => {
//     console.log("MOUNTED")
//   }
// })

Vue.component('pagination', Pagination);
Vue.component('preview', Preview);

new Vue({
  el: '#app',
  render: h => h(App),
  mounted: () => {
    window.$('.sidenav').sidenav();
    window.$('.modal').modal({
      onOpenEnd() {
        window.$('select').formSelect();
      }
    });
    // window.$('select').formSelect();
  },
  router,
  // Slideout
})
