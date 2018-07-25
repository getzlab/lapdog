<template>
  <div id="app">
    <header>
      <div class="sidenav" id="slide-out" style="width: 50% !important;">
        <div class="user-view center-align">
          <h3>Workspaces</h3>
        </div>
        <div class="container">
          <div class="collection">
            <a class="collection-item blue-text text-darken-4"><i class="material-icons">add_box</i> Create new Workspace</a>
          </div>
          <div class="divider">
          </div>
          <div class="collection">
            <div class="collection-item" v-if="!workspaces">
              <div class="progress">
                <div class="indeterminate"></div>
              </div>
            </div>
            <div class="collection-item" v-else>
              <div class="input-field">
                <input id="search_bar" type="text" v-model="search">
                <label for="search_bar">Search Workspaces</label>
              </div>
            </div>
            <router-link class="blue-text text-darken-4 collection-item sidenav-close" v-for="workspace in filtered_workspaces" active-class="active blue lighten-1"
              :to="{name: 'workspace', params: {namespace: workspace.namespace, workspace: workspace.name}}">
              {{workspace.namespace}}/{{workspace.name}}
            </router-link>
          </div>
        </div>
      </div>
      <nav class="blue darken-2">
        <div class="nav-wrapper">
          <!-- <form>
            <div class="input-field">
              <input type="text" id="submission-search">
              <label for="submission-search">Search Submissions</label>
            </div>
          </form> -->
          <router-link href="#" class="brand-logo right link" :to="'/'">Lapdog</router-link>
          <ul class="left">
            <li>
              <a data-target="slide-out" class="sidenav-trigger show-on-large"><i class="material-icons">menu</i></a>
            </li>
          </ul>
        </div>
      </nav>
      <!-- <div class="navbar-fixed">
        <nav class="blue darken-2">
          <div class="nav-wrapper">
            <div class="row">
              <form>
                <div class="input-field">
                  <input type="text" id="submission-search">
                  <label for="submission-search">Search Submissions</label>
                </div>
              </form>
              <div class="col s2">
                <ul class="">
                  <li>
                    <a data-target="slide-out" class="sidenav-trigger show-on-large"><i class="material-icons">menu</i></a>
                  </li>
                </ul>
              </div>
              <div class="col s1 offset-s9">
                <router-link href="#" class="brand-logolink" :to="'/'">Lapdog</router-link>
              </div>
            </div>
          </div>
        </nav>
      </div> -->
      <!-- <ul id="slide-out" class="sidenav">
        <li><div class="user-view">
          <h3>Workspaces</h3>
        </div></li>
        <li><i class="material-icons">add_box</i> Create new Workspace</li>
      </ul> -->

      <!-- <a data-target="slide-out" class="sidenav-trigger left">Menu</a> -->
    </header>
    <main>
      <aside class="sidebar">
      </aside>
      <div class="content container">
        <router-view></router-view>
      </div>
    </main>
    <footer class="grey lighten-1">
      <div class="row">
        <div class="col s2 offset-s2">
          Powered by <a href="https://github.com/broadinstitute/dalmatian">Dalmatian</a>
        </div>
        <div v-if="acct" class="col s7">
          Your google service account: {{acct}}
        </div>
      </div>
    </footer>
  </div>
</template>

<!-- <script type="text/javascript">
  console.log(window.$)
</script> -->

<style lang="css">
   /* label focus color */
   .input-field input[type=text]:focus + label {
     color: #1565c0 !important;
   }
   /* label underline focus color */
   .input-field input[type=text]:focus {
     border-bottom: 1px solid #1565c0 !important;
     box-shadow: 0 1px 0 0 #000 !important;
   }
   /* valid color */
   .input-field input[type=text].valid {
     border-bottom: 1px solid #1565c0 !important;
     box-shadow: 0 1px 0 0 #000 !important;
   }
</style>

<script>
import axios from'axios'
export default {
  data() {
    return {
      workspaces: null,
      acct: null,
      search: ''
    }
  },

  computed: {
    filtered_workspaces() {
      return !this.workspaces ? null : this.workspaces.filter(ws => {
        return this.search.length == 0 || (ws.namespace+'/'+ws.name).toLowerCase().includes(this.search.toLowerCase())
      })
    }
  },

  created() {
    this.getStatus();
    this.getServiceAccount();
  },

  methods: {
    getStatus() {
      axios.get('http://localhost:4201/api/v1/workspaces')
        .then(response => {
          this.workspaces = response.data;
          console.log(this.workspaces[0])
        })
        .catch(error => {
          console.error("FAIL!")
        })
    },
    getServiceAccount() {
      axios.get('http://localhost:4201/api/v1/service-account')
        .then(response => {
          this.acct = response.data
        })
        .catch(error => {
          console.error("FAIL!")
        })
    }
  }
}
</script>
